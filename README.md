# 浅尝开源集群运算框架Apache Spark 

## 前言
Apache Spark是一个开源集群运算框架，我们将用一个简单的例子了解它。这里，我们在Arch Linux操作系统下进行实验。

## Apache Spark简介(摘自[维基百科词条](https://zh.wikipedia.org/wiki/Apache_Spark))
Apache Spark是一个开源集群运算框架，最初是由加州大学柏克莱分校AMPLab所开发。相对于Hadoop的MapReduce会在运行完工作后将中介数据存放到磁盘中，Spark使用了内存内运算技术，能在数据尚未写入硬盘时即在内存内分析运算。Spark在内存内运行程序的运算速度能做到比Hadoop MapReduce的运算速度快上100倍，即便是运行程序于硬盘时，Spark也能快上10倍速度。Spark允许用户将数据加载至集群内存，并多次对其进行查询，非常适合用于机器学习算法。

## 问题
一个简单的例子，有一些小朋友，他们想要吃水果，我们用一张表列出小朋友们的需求。
名字|水果
----|----
Sam|apple,pear
Amy|apple
Jim|strawberry
Jackie|watermelon
Harry|strawberry
Lily|watermelon,pear
Tom|watermelon

我们想知道，每一种水果，有哪几位小朋友需要吃。我们可以使用Apache Spark来解决这个问题。

## 安装Apache Spark
通过Arch Linux的用户源(AUR, Arch User Repository)，可以轻松地安装Apache Spark包。

```bash
首先我们需要安装*yaourt*
# 添加archlinuxcn镜像源
sudo vim /etc/pacman.conf
# 任意位置粘贴以下内容
[archlinuxcn]
Server = https://mirrors.ustc.edu.cn/archlinuxcn/$arch
# 更新源
sudo pacman -Sy
# 安装java8
sudo pacman -S jre8-openjdk
# 安装*Apache Hadoop*
yaourt -S apache
# 遇到选择语句，按回车键即可
# 安装*Apache Spark*
yaourt -S apache-spark
# 遇到选择语句，按回车键即可
# 安装pyspark
sudo pip install pyspark
```

## 实验 

### 数据 
文件名: fruit.txt
```
Sam:apple,pear
Amy:apple
Jim:strawberry
Jackie:watermelon
Harry:strawberry
Lily:watermelon,pear
Tom:watermelon
```

### 代码

文件名: fruit.py
```python
#coding=utf-8
from pyspark import SparkConf, SparkContext

## 应用名称 
APP_NAME = "分水果"

def main(sc):
    text = sc.textFile("fruit.txt")
    #map sam:apple,pear
    def parseText(t):
        '''
            生成(水果, 人名)的数组
            如Sam: apple, pear 
            得到数组: [(apple, sam), (pear, sam)]
        '''
        res = []
        for line in t.split('\n'):
            name, fs = line.split(':')
            for fruit in fs.split(','):
                res.append((fruit, name))
        return res
    people = text.flatMap(parseText)
    wc = people.map(lambda x : (x[0],x[1])) # Map
    def mergePeople(a, b):
        sa = set(a.split(','))
        sb = set(b.split(','))
        return ','.join([str(u) for u in (sa | sb)])
    res = wc.reduceByKey(mergePeople) # Reduce
    res.saveAsTextFile("out") # 保存结果到文件夹out

if __name__ == "__main__":
    # 配置Apache Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    main(sc)
```

输入命令
```bash
spark-submit fruit.py
```

命令输入后，Spark将开始运行。

## 实验原理
`parseText`函数读入字符串，将输入文本转为`(水果, 人名)`的数组，其中水果为关键字`(key)`
`map`函数: 将一个元素变为`(key, value)`对，例子中用匿名函数`lambda x : x[0], x[1]`将元素转为`(x[0], x[1])`对，其中`x[0]`为`key`, `x[1]`为`value`
`reduceByKey`函数: 合并`key`相同的两个元组，如合并`("apple", "Sam")`, `("apple", "Amy")`两个元组，得到`("apple", "Sam,Amy")`

## 实验结果
我们将看见, 在工作目录下(fruit.py)生成了一个out文件夹，进入文件夹，里面存放着名为part-xxxxx的文件(如: part-00000, part-00001)和一个_SUCCESS文件。

输入命令
```bash
cat part-* > result.txt
```
结果将存放到*result.txt*文件里，文件的内容为:
```
('pear', 'Lily,Sam')
('watermelon', 'Tom,Jackie,Lily')
('apple', 'Amy,Sam')
('strawberry', 'Harry,Jim')
```
显示了每种水果有哪些小朋友想吃。

统计的结果正确:)
