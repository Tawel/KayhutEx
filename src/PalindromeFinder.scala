import org.apache.spark.{SparkConf, SparkContext}

object PalindromeFinder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Tal").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Reading the book and skipping the empty words
    val text = sc.textFile("Resources/book.txt").flatMap(line => line.split(" ")).filter(!_.isEmpty)
    // Only the palindrome words are interesting
    val palindromes = text.filter(word => isPalindrome(word))

    // Doing word count and sort
    val sorted = palindromes.map(word => (word, 1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .collect()

    // Getting the top most popular words that are palindromes
    val top10 = sorted.take(10);

    // Getting the top most popular words that are palindromes with more then one letter (maybe more interesting to inspect)
    val bigTop10 = sorted.filter(_._1.length>1).take(10)

    println("Total top 10")
    top10.foreach(println(_))

    println("")
    println("Big top 10")
    bigTop10.foreach(println(_))

    sc.stop()
  }

  // Recursively checking if a given word is a palindrome
  def isPalindrome(a:String): Boolean={
    if (a.length == 1) return true
    if (a.length == 2 && a(0) == a(1)) return true
    if (a(0) != a.last) return false

    isPalindrome(a.substring(1, a.length - 1))
  }
}
