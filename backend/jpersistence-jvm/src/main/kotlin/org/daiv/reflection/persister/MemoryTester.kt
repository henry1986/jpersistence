package org.daiv.reflection.persister

data class TestObj(val x:Int)

fun main(args: Array<String>) {
    val persister = Persister("memTest.db")
    val table = persister.Table(TestObj::class)
    table.persist()
    if(!table.exists(5)){
        table.insert(TestObj(5))
    }
//    (0..100000).forEach{
//        val x = table.read(5)
//    }

    while(true){
        val x = table.read(5)
        Thread.sleep(10L)
//        System.gc()
    }
    persister.close()
}