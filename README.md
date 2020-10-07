## 1. Overview
This is a simple relational database(named SimpleDB) implementation in Java. And this project is a playgorund-level database implementation described in the book "Database Design And Implementation" written by Edward Sciore, Boston College.

Just for educational use, I will follow the author to reimplemente the SimpleDB. I have commented some code in Chinese for convenient reading and trasnlated mainly chapters of part 3 in Chinese. You are also highly recommended to take a look about the original textbook which is really easy to understand.

这是一个简单的关系数据库（名为simpleDB）实现，Java语言实现，这是《Database Design And Implementation》作者Edward Sciore在书中提供的一个游乐场级别的数据库实现。 为了方便学习使用，我会跟随作者的脚步去再次实现这个数据库。为方便大家阅读代码，我已经将部分关键代码添加了中文注释，并且尝试翻译了原书第3部分中的绝大部分章节，但仍然建议配合原书一起使用，因为相较于中文，英文没有那么地晦涩并且歧义更少，理解起来更简单。

## 2. 架构

SimpleDB的整体架构如下图所示，下层组件为上层组件提供服务：

![](part3/part3-01.png)

## 3. 特点

### 3.1 磁盘和文件管理
- 将文件块作为磁盘访问的最基本单元，缩短磁盘访问时间。
- 文件块中当前只支持int和string类型的读写。

### 3.2 内存管理
- 通过维护缓冲池来固定最常使用的用户数据块，目前支持Naive缓冲页替换算法。

### 3.3 事务管理
- 支持日志来恢复数据库，采用的是undo-only恢复算法。
- 运用xlock和slock来控制多事务对块的并发访问。
- 事务对锁一直持有，直到事务commit或rollback。

### 3.4 记录管理
- 当前支持定长字段，定长记录。
- 一个记录文件中存储的是同类(homogeneous)的记录。
- 当前支持非跨块的记录(non spanned records)。
- 给客户端提供记录文件粒度的记录增、删、查、改方法，隐藏了底层的块、页细节。

### 3.5 元数据管理
- SimpleDB实现了4类元数据：
  1. 表的元数据，描述的是某张表的信息，例如该表每条记录的长度，每个字段的偏移量，类型。
  2. 视图的元数据，描述的是每个视图的属性，例如视图的名称和定义。
  3. 索引的元数据，描述的是每个索引的信息，包含索引名、被索引的表名、被索引的字段名。
  4. 数据统计的元数据，描述的是每张表的占用块数，已经各字段的值分布情况。
- 表的元数据存放在系统的catalog表tblcat和fldcat中，一张来存放表粒度的信息（例如记录长度），另一种表来存放表中各字段信息（例如字段名、字段长度、字段的类型）。表名和字段名最长字符限定为20。
- 视图的元数据存放在系统的catalog表viewcat中，视图的定义最长字符数我们限定成了150。
- 索引的元数据存放在系统的catalog表idxcay中，包含索引名、被索引的表名、被索引的字段名。
- 数据统计信息没有用表来存，而是在系统每次启动的时候重新计算出，对于小型的数据库，这一统计计算时间不是很长，因此不会拖长整个系统的启动时间。

### 3.6 查询处理
- 基于流水线的方式实现Scan。目前支持TableScan、SelectScan、ProductScan和ProjectScan,分别对应SQL语句中的表、谓词筛选、笛卡尔积和输出Select列名。
- 对于一个SQL，可能存在多个等效的查询树，planner会比较这些查询树对应的Scan的执行代价，选择代价最低的那个。
- 目前支持形如"A = c"和"A = B"形式的谓词，前者代表例如`where StuId=1`的形式，而后者代表例如`where Stu.StuId = Exam.Id`的形式。

### 3.7 SQL解析
- SimpleDB中只支持SQL中的子集，具体有：
    1. 简单的查询，单表多表均支持
    2. 增删改记录
    3. 创建表、视图、索引
    4. 只支持where谓词，不支持group by等    
- 采用递归下降解析法(recursive descent)解析SQL语句。

### 3.8 SQL Planning
- 目前实现了最简单的planning算法（包括query planning算法和update planning算法），没有作SQL语义验证和plan代价分析。
- 设计了好了Planner的接口，增强代码的plug-and-play capability。

### 3.9 C/S 通信

- 通过Java中的RMI机制来实现客户端和服务端之间的通信。
- 服务端的每个远程实现类对象都在一个独自的线程中执行，等待客户端通过存根对象发送消息，SimpleDB启动代码会创建一个`RemoteDriver`类型的远程实现对象，并把这个对象的存根对象注册到RMI的注册表中。
- 当客户端想要连接到数据库系统时，会先通过RMI注册表得到存根对象，并按照JDBC提供接口进行数据访问，目前实现了`SimpleConnection`,`SimpleStatement`等，分别对应JDBC中的`Connection`,`Statement`。

### 3.10 索引

- 实现了`静态hash索引`和`B树索引`两种方式

___

### Q&A
#### 1. 本书的面向的读者？
本书的面向的读者是那些想要学习或研究数据库原理和底层实现的人，你最好对数据库的基本概念有个大概的了解，例如基本的SQL语句、数据库设计的几种范数、数据库的ACID原则等等，细节不清楚没关系，这本书就是给你展示细节的。本书假设你对操作系统、编译原理中的一些基本概念略有耳闻，例如磁盘访问、并发、多线程等等。

#### 2. 关于错误
本书是译者在阅读英文版原书时翻译的手稿或者说笔记，难免会有很多错误，必然存在我的理解错误、口误、输入错误、翻译不准确等众多问题，如果你有任何问题，请先仔细思考或查阅相关资料，如果你的确认为有问题，非常欢迎提出issue讨论交流。

#### 3. 译本进度
目前只翻译了第12—21章，由于本人时间有限，因此会不定时地更新本译本，欢迎任何人共同加入到翻译此书的工作中来，如果你有意向，email联系，也欢迎直接pull request。

#### 4. 如何获得本书？
1. 你可以直接打开[SimpleDB中文版gitbook](https://liuzhian.gitbook.io/simpledb/)查看。
2. 你也可以在本地安装nodejs和gitbook，并且编译成PDF文件格式查看。

***本仓库所有译本只是本人学习时所翻译，默认可以转载，但请注明出处，也请勿用于任何商业用途。***

译者：Liu Zhian

email：csliuzhian@mail.scut.edu.cn

___
### 打赏
你的支持将会是我最大的动力！打赏记得备注哟~ ¡Salud :beers: :beers: :beers: ​

![](myQRcode.png)