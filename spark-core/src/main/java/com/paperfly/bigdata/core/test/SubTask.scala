package com.paperfly.bigdata.core.test

class SubTask extends Serializable {
    var datas : List[Int] = _
    var logic : (Int)=>Int = _

    // 计算
    def compute() = {
        datas.map(logic)//和Java的map函数一样，循环遍历每一个数据，做处理
    }
}
