package main

import scala.util.parsing.combinator._
import java.text.SimpleDateFormat
import java.util.Scanner
import akka.actor.{ Props, ActorSystem, Actor }

class Grammar extends JavaTokenParsers {
  
  val system = ActorSystem.create("DB") 
  val ts = system.actorOf(Props(new TableSet()), "Database")
 
  def statement: Parser[Command] = (admin_statement | ddl_statement | dml_statement | query_statement) <~";"// | transaction_statement)~";"
  def admin_statement: Parser[Command] = exit | print
  def print: Parser[Command] = "(?i)print\\s+".r ~ ("(?i)dictionary" | table_name) ^^ (x => Print(x._2.toString))
  def exit: Parser[Command] = "(?i)exit".r ^^ (x=>Exit())
  
  def ddl_statement: Parser[Command] = define_table
  def define_table: Parser[Command] = "(?i)define\\s+table".r ~ table_name ~ "(?i)having\\s+fields".r ~ "(" ~ extended_field_list ~")" ^^  {case _~name~_~_~fields~_ => Define_Table(name, fields)}
 //^^ (x => Define_Table(x._1._1._1.toString, x._2))
  def dml_statement: Parser[Command] = insert | update | delete
  def delete: Parser[Command] = "(?i)delete\\s+".r ~ table_name ~ opt(where) ^^  {case _~name~where => Delete(name, where)}
  def insert: Parser[Command] = "(?i)insert".r ~ '(' ~ value_list ~ ')'~"(?i)into\\s+".r ~ table_name ^^  {case _~_~values~_~_~name => Insert(values, name.toString)}
  def update: Parser[Command] = "(?i)update\\s+".r ~ table_name ~ "(?i)set\\s+".r ~ field_name ~ "=" ~ value ~ opt(where) ^^ {case _~name~_~fieldName~_~value~where => Update(name, fieldName, value, where)}
  
  def query_statement: Parser[Query] = selection// | projection | join | intersection | union | minus | sort
  def selection: Parser[Query] = "(?i)select\\s+".r ~ query_list ~ opt(where) ^^ {case _~qList~where => Select(qList, where)}
//  def projection: Parser[Query] = "(?i)project\\s+".r ~ query_list ~ "(?i)over\\s+".r ~ field_list ^^ {case _~qList~_~fList => Project(qList, fList)}
//  def join: Parser[Query] = "(?i)join\\s+".r ~ query_list ~ "(?i)and\\s+".r ~ query_list ^^ { case _~qList1~_~qList2 => Join(qList1, qList2)}
//  def intersection: Parser[Query] = "(?i)intersect\\s+".r ~ query_list ~ "(?i)and\\s+".r ~ query_list ^^ { case _~qList1~_~qList2 => Intersect(qList1, qList2)}
//  def union: Parser[Query] = "(?i)union\\s+".r ~ query_list ~ "(?i)and\\s+".r ~ query_list ^^ { case _~qList1~_~qList2 => Union(qList1, qList2)}
//  def minus: Parser[Query] = "(?i)minus\\s+".r ~ query_list ~ "(?i)and\\s+".r ~ query_list ^^ { case _~qList1~_~qList2 => Minus(qList1, qList2)}
//  def sort: Parser[Query] = "(?i)order\\s+".r ~ query_list ~ "(?i)by\\s+".r ~ field_name ^^ { case _~qList1~_~fieldName => Sort(qList1, fieldName)}
  def transaction_statement: Parser[Any] = begin | end | pause
  def begin: Parser[Any] = "(?i)begin".r
  def end: Parser[Any] = "(?i)end".r
  def pause: Parser[Any] = "(?i)pause".r ~ "(" ~ integer ~ ")" 
  
  def where: Parser[WhereClause] = "(?i)where\\s+".r ~ boolean_expression ^^ (x => x._2)
  def boolean_expression: Parser[WhereClause] = field_name ~ relop ~ value ^^ {case f~r~v => new WhereClause(f,r,v)}
  
  def extended_field_list: Parser[List[Field]] = repsep(field, ",\\s+".r)
  def table_name: Parser[String] = "[a-zA-Z]+".r
  //def query_table: Parser[QueryTable] = "[a-zA-Z]+".r ^^ (x => new QueryTable(x.toString))
  def query_list: Parser[Any] = (query_statement | table_name)
  def field_name: Parser[String] = """[a-zA-Z]+""".r
  def type_ : Parser[Any] = "integer" | "date" | "real" | "varchar" | "boolean"
  def field: Parser[Field] = field_name~type_ ^^ (x => FieldFactory.initField(x._2.toString, x._1.toString))//new IntField(x._1.toString))
  def field_list: Parser[List[String]] = repsep(field_name, ",\\s+".r)
  
  def value: Parser[Value] = (real | integer | date | boolean | string_expression)
  def value_list: Parser[List[Value]] = repsep(value, ",\\s+".r)
  def char: Parser[Any] = """[a-zA-Z0-9]""".r
  def integer: Parser[IntValue] = """[0-9]+""".r ^^ (x => new IntValue(x.toInt))
  def real: Parser[RealValue] = (integer~opt('.')~integer | '.'~integer) ^^ {case x~Some('.')~y => new RealValue((x.toString + '.' + y).toDouble)
  																			 case '.'~x => new RealValue(('.' + x.toString).toDouble)
  																			 case x => new RealValue(x.toString.toDouble)}
  def date: Parser[DateValue] = "'"~>"[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]".r<~"'" ^^ (x => new DateValue(new SimpleDateFormat("MM/dd/yyyy").parse(x.toString)))
  def d: Parser[Any] = """[0-9]""".r
  def string: Parser[Any] = """[a-zA-Z0-9]+""".r
  def string_expression: Parser[VarValue] = ("\'"~>string<~"\'") ^^ (x => new VarValue("\'" + x.toString() + "\'"))
  def boolean: Parser[BoolValue] = ("true" | "false") ^^ (x => new BoolValue(x.toBoolean))
  def relop: Parser[String] = ("=" | "!=" |"<=" | ">=" | "<" | ">")

}

class WhereClause(val fieldName: String, val relop: String, val value: Value)  {
  def compareTo(pair: (Field, Value)): Boolean = {
    if(pair._1.getName == fieldName) {
	    val comp = pair._2.compareTo(value)
	    relop match {
	      case "=" => comp == 0
	      case ">" => comp > 0
	      case "<" => comp < 0
	      case ">=" => comp >= 0
	      case "<=" => comp <= 0
	      case "!=" =>  comp != 0
	     }
    }
    else false
  }
}

object ParseExpr extends Grammar {
  def parse(s: String): Unit = {
	//println("input : " + s)
	
	val p = parseAll(statement, s)
	if(p.successful) println("Valid command.")
	else println("Invalid command.")
	p match {
	  case Failure(_,_) => println("Failure.")
	  case Success(c,_) => ts ! c//c.execute() //match {
//	    case t: Table => println(t)
//	    case Unit => println(p)
//	  }
	}

	//println(p)
  }
  
  def checkCommand(s: String): ParseResult[Command] = {
    parseAll(statement, s)
  }
}

object Main extends App {
  run
  def run(): Unit = {
    val sc: Scanner = new Scanner(System.in)
    TableSet.fromXML
    print('>')
    while(true) {
      var in = sc.nextLine()
      while(!in.contains(';'))
        in=in.concat(" " + sc.nextLine())
      ParseExpr.parse(in)
//      if(p.successful) println("Valid command.")
//		else println("Invalid command.")
//		p match {
//		  case Failure(_,_) => println("Failure.")
//		  case Success(c: Command,_) => c.execute() match {
//		    case t: Table => println(t)
//		  }
//		}
    }
  }
  
  
  
}