import akka.actor._
import akka.routing.RoundRobinRouter
import akka.util.Duration
import akka.util.duration._

import scala.annotation.tailrec

sealed trait PiMessage

case object Calculate extends PiMessage

case class Work(start: Int, nrOfElements: Int) extends PiMessage

case class Result(value: Double) extends PiMessage

case class PiApproximation(pi: Double, duration: Duration)

object Pi {
  def main (args: Array[String]) {
    calculate(4, 10000, 10000)
  }

  class Worker extends Actor {

    def calculatePiFor(start: Int, nb: Int): Double = {
      @tailrec
      def go(acc: Double, n: Int): Double = {
        if (n >= start + nb) acc
        else go(acc + 4 * scala.math.pow(-1, n) / (2 * n + 1), n + 1)
      }

      go(0, start)
    }

    def receive = {
      case Work(start, nrOfElements) => sender ! Result(calculatePiFor(start, nrOfElements))
    }
  }

  class Master(nrOfWorkers: Int, nrOfMessages: Int, nrOfElements: Int, listener: ActorRef) extends Actor {

    var pi: Double = _
    var nrOfResults: Int = _
    val start: Long = System.currentTimeMillis

    val workerRouter = context.actorOf(
      Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")

    def receive = {
      case Calculate =>
        for (i <- 0 until nrOfMessages) workerRouter ! Work(i * nrOfElements, nrOfElements)
      case Result(value) =>
        pi += value
        nrOfResults += 1
        if (nrOfResults == nrOfMessages) {
          // Send the result to the listener
          listener ! PiApproximation(pi, duration = (System.currentTimeMillis - start).millis)
          // Stops this actor and all its supervised children
          context.stop(self)
        }
    }
  }

  class Listener extends Actor {
    def receive = {
      case PiApproximation(pi, duration) =>
        println("pi: %f in %s".format(pi, duration))
        context.system.shutdown()
    }
  }

  def calculate(nrOfWorkers: Int, nrOfMessages: Int, nrOfElements: Int) = {
    val system = ActorSystem("Pi")
    val listener = system.actorOf(Props[Listener], name = "listener")
    val master = system.actorOf(Props(new Master(nrOfWorkers, nrOfMessages, nrOfElements, listener)), name = "master")

    master ! Calculate
  }
}