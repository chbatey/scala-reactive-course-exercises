package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("two elements") = forAll { (a: Int, b: Int) =>
    val smaller = if (a < b) a else b
    val h = insert(b, insert(a, empty))
    findMin(h) == smaller
  }

  property("two elements inserted the other way") = forAll { (a: Int, b: Int) =>
    val smaller = if (a < b) a else b
    val h = insert(a, insert(b, empty))
    findMin(h) == smaller
  }

  property("Adding and removing one element results in empty") = forAll { a: Int =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }

  property("deleting the minimum") = forAll { (a: Int, b: Int) =>
    val larger = if (a > b) a else b
    val h = deleteMin(insert(b, insert(a, empty)))
    findMin(h) == larger
  }


  property("Melding with empty makes no difference to min") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    val melded = meld(h, empty)
    findMin(melded) == m
  }

  property("Heap size") = forAll { (list: List[Int]) =>

    val heap = list.foldLeft(empty)((h, n) => insert(n, h))

    def heapSize(h: H): Int = {
      if (isEmpty(h)) {
        0
      } else {
        1 + heapSize(deleteMin(h))
      }
    }

    heapSize(heap) == list.size
  }

  property("Find min always returns the min") = forAll { (h: H) =>
    def getAndCheckMin(h: H, last: Int) = {
      if (!isEmpty(h)) {
        val min = findMin(h)
        assert(min >= last)
      }
    }
    if (!isEmpty(h)) {
      val firstMin = findMin(h)
      val newHeap = deleteMin(h)

      if (!isEmpty(newHeap)) {
        val secondMin = findMin(h)
        assert(firstMin <= secondMin)
        getAndCheckMin(deleteMin(newHeap), secondMin)
      }
    }
    true
  }


  lazy val genHeap: Gen[H] = for {
    newValue <- arbitrary[Int]
    heap <- oneOf(empty, genHeap)
  } yield insert(newValue, heap)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
