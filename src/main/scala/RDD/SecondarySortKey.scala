package RDD

//                                                                                          网络传输的时候需要序列化
class SecondarySortKey(val word: String, val count: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if(this.word.compareTo(that.word) != 0) {
      this.word.compareTo(that.word)
    }else{
      this.count - that.count
    }
  }
}
