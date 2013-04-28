package main

import akka.actor.{ Props, ActorSystem, Actor }
import main.TelnetServer._

object Database {
  val system = ActorSystem.create("Database") 
  def main(args: Array[String]) {
    val db = system.actorOf(Props(Database()), "Database")
    val server = system.actorOf(Props(new TelnetServer(db)), "Telnet")
    TableSet.fromXML
  }
  case class Message(msg : String)
  case class Exit
  def apply() = new Database()
}


class Database extends Actor {
  import Database._
  import TelnetServer._
   def receive = {
    case Message(msg) =>
      if(msg == "exit") Exit
      else {
    	  sender ! Response(msg)
    	  ParseExpr.parse(msg)
      }
  }
}
