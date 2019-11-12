package scala.com.bgc

import com.bgc.Main
import com.bgc.LoadData
import common.Utils
import org.scalatest.FunSuite

import scala.collection.mutable.Stack


/**
  * Created by jnayak on 12/11/2019.
  */
class imdb extends FunSuite {


  val df = new LoadData
  var names = "/Users/jnayak/git/imdb/imdb/src/test/resources/name.basics.tsv.gz"
  var titles = "/Users/jnayak/git/imdb/imdb/src/test/resources/title.basics.tsv.gz"
  var ratings = "/Users/jnayak/git/imdb/imdb/src/test/resources/title.ratings.tsv.gz"

  val top20 = df.readData("ratings", "/Users/jnayak/git/imdb/imdb/src/test/resources/top20.tsv", 32)
  val creditors = df.readData("ratings", "/Users/jnayak/git/imdb/imdb/src/test/resources/creditors.tsv", 32)



  test("top20") {
    val expected = (top20, creditors)
    val dfs = new Main
    val result = dfs.getDfs(names,titles,ratings)
    assert(expected === result)
  }

}

