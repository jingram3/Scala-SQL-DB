package main

import scala.util.parsing.combinator._
import akka.actor.{ Props, ActorSystem, Actor }

abstract class Command {
  def execute()
}

abstract class Query extends Command {
  def executor(): Table
}

  case class Print(name: String) extends Command {
	def execute(): Unit = {
	  if(name == "dictionary") println(TableSet)
	  else {
	    TableSet.get(name) match {
	      case Some(x) => x.printTable()
	      case None => println("Table not found.")
	    }
	  }
	}
  }
  case class Exit() extends Command {
    def execute(): Unit = {
      println("Exiting and writing to XML.")
      xml.XML.save("database.xml", TableSet.toXML)
	  System.exit(0)
    }
  }
  case class Define_Table(name: String, fields: List[Field]) extends Command {
    def execute(): Unit = {
      val t = new Table(name, fields, List())
      TableSet.addTable(t)
      //println(t)
    }	    
  }
  case class Delete(name: String, where: Option[WhereClause]) extends Command {
    def execute(): Unit = {
      //ts ! this
      TableSet.delete(name, where)
    }
  }
  case class Insert(values: List[Value], name: String) extends Command {
    def execute(): Unit = {
      TableSet.insert(values, name)
    }
  }
  case class Update(name: String, fieldName: String, value: Value, where: Option[WhereClause]) extends Command {
    def execute(): Unit = {
      TableSet.update(name, fieldName, value, where)
    }
  }
  case class Select(query_list: Any, where: Option[WhereClause]) extends Query {
    def execute(): Unit = {
      executor.printTable
    }
      
    def executor(): Table = {
      query_list match {
        //case Select(x, y) => TableSet.select((new Select(x, y).executor), where)
        case (q: Query) => TableSet.select(q.executor, where)
        case x => TableSet.select(x.toString, where)
      }
    }
  }
  
  case class Project(query_list: Any, field_list: List[String]) extends Query {
    def execute(): Unit = {
      executor.printTable
    }
      
    def executor(): Table = {
      query_list match {
        case (q: Query) => TableSet.project(q.executor, field_list)
        case x => TableSet.project(x.toString, field_list)
      }
    }
  }
  
  case class Join(query_list: Any, query_list2: Any) extends Query {
    def execute(): Unit = {
      executor.printTable
    }
      
    def executor(): Table = {
      (query_list, query_list2) match {
        case (q1: Query, q2: Query) => q1.executor join q2.executor
        case (q1: Query, b) => q1.executor join TableSet.get(b.toString).get
        case (a, q2: Query) => TableSet.get(a.toString).get join q2.executor
        case (a, b) => TableSet.join(a.toString, b.toString)
      }
    }
  }
  
  case class Intersect(query_list: Any, query_list2: Any) extends Query {
    def execute(): Unit = {
      executor.printTable
    }
      
    def executor(): Table = {
      (query_list, query_list2) match {
        case (q1: Query, q2: Query) => q1.executor intersect q2.executor
        case (q1: Query, b) => q1.executor intersect TableSet.get(b.toString).get
        case (a, q2: Query) => TableSet.get(a.toString).get intersect q2.executor
        case (a, b) => TableSet.intersect(a.toString, b.toString)
      }
    }
  }
  
  case class Union(query_list: Any, query_list2: Any) extends Query {
    def execute = executor.printTable
      
    def executor(): Table = {
      (query_list, query_list2) match {
        case (q1: Query, q2: Query) => q1.executor union q2.executor
        case (q1: Query, b) => q1.executor union TableSet.get(b.toString).get
        case (a, q2: Query) => TableSet.get(a.toString).get union q2.executor
        case (a, b) => TableSet.union(a.toString, b.toString)
      }
    }
  }
  
  case class Minus(query_list: Any, query_list2: Any) extends Query {
    def execute = executor.printTable
      
    def executor(): Table = {
      (query_list, query_list2) match {
        case (q1: Query, q2: Query) => q1.executor minus q2.executor
        case (q1: Query, b) => q1.executor minus TableSet.get(b.toString).get
        case (a, q2: Query) => TableSet.get(a.toString).get minus q2.executor
        case (a, b) => TableSet.minus(a.toString, b.toString)
      }
    }
  }
  
  
  case class Sort(query_list: Any, fieldName: String) extends Query {
    def execute = executor.printTable
      
    def executor(): Table = {
      query_list match {
        case (q: Query) => q.executor sort fieldName
        case x => TableSet.sort(x.toString, fieldName)
      }
    }
  }

  
