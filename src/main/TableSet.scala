package main

import akka.actor.{ Props, ActorSystem, Actor }

class TableSet extends Actor {
  import TableSet._
  def receive = {
    case c: Command => c.execute(); print('>')
  }
}

object TableSet {
  
  var tables = Map[String, Table]()
  
  def addTable(t: Table): Unit = {
    tables = tables + (t.getName() -> t)
  }
  
  override def toString(): String = tables.mkString("\n")
  
  def get(name: String): Option[Table] = {
    tables.get(name)
  }
  
  def insert(values: List[Value], name: String): Unit = {
    if(tables.contains(name)) {
      val t = get(name).get
      tables = tables - name
      tables = tables + (name -> t.insert(values))
    }
    else {
      println("insertion failed")
    }
  }
  
  def update(name: String, fieldName: String, value: Value, where: Option[WhereClause]): Unit = {
    if(tables.contains(name)) {
      val t = get(name).get
      tables = tables - name
      tables = tables + (name -> t.update(fieldName, value, where))
    }
    else {
      println("update failed")
    }
  }
  
  def delete(name: String, where: Option[WhereClause]): Unit = {
    if(tables.contains(name)) {
      val t = get(name).get
      tables = tables - name
      tables = tables + (name -> t.delete(where))
    }
    else {
      println("update failed")
    }
  }
  
  def select(name: String, where: Option[WhereClause]): Table = {
    if(tables.contains(name)) {
      val t = get(name).get
      t.select(where)
    }
    else {
      new Table("",Nil,Nil)
    }
  }
  
  def select(table: Table, where: Option[WhereClause]): Table = {
    table.select(where)
  }
  
  def project(name: String, fields: List[String]): Table = {
    if(tables.contains(name)) {
      val t = get(name).get
      t.project(fields)
    }
    else {
      new Table("",Nil,Nil)
    }
  }
  
  def project(table: Table, fields: List[String]): Table = {
    table.project(fields)
  }
  
  def join(name1: String, name2: String): Table = {
    query(name1, name2, ((x, y) => x.join(y)))
  }
  
  def intersect(name1: String, name2: String): Table = {
    query(name1, name2, ((x, y) => x.intersect(y)))
  }
  
  def union(name1: String, name2: String): Table = {
    query(name1, name2, ((x, y) => x.union(y)))
  }
  
  def minus(name1: String, name2: String): Table = {
    query(name1, name2, ((x, y) => x.minus(y)))
  }
  
  def sort(name: String, fieldName: String): Table = {
    if(tables.contains(name)) {
      val t = get(name).get
      t.sort(fieldName)
    }
    else {
      new Table("",Nil,Nil)
    }
  }
  
  def query(name1: String, name2: String, f: ((Table, Table) => Table)): Table = {
    if(tables.contains(name1) && tables.contains(name2)) {
      val t = get(name1).get
      val t2 = get(name2).get
      f(t, t2)
    }
    else {
      new Table("",Nil,Nil)
    }
  }
  
  def toXML: scala.xml.Node = {
    <database>
	  {for ((name, table) <- tables) yield table.toXML}
	</database>
  }
  
  def fromXML: Unit = {

    try {
	    val src = scala.io.Source.fromFile("database.xml")
	    val cpa = scala.xml.parsing.ConstructingParser.fromSource(src, false)
	    val doc = cpa.document()
	    val dtd = doc.dtd
	    val ele = doc.docElem
    	ele match {
	      case <database>{tables @ _*}</database> =>
	        for (table <- tables) {
	          val name = (table \\ "tablename").text
	          val fields = (for (field <- (table \\ "field")) yield {
	            FieldFactory.initField((field \\ "fieldtype").text, (field \\ "fieldname").text)
	          }).toList
	          val rows = for (row <- (table \\ "row")) yield {
	            val cols = for (col <- (row \\ "column")) yield {
	              col.text
	            }
	            val vals = fields.zip(cols).map(x => x._1.getValue(x._2))
	            new Row(vals, fields)
	          }
	          addTable(new Table(name, fields, rows.toList))
	        }
	    }
    }
    catch {
      case e => println(e.getMessage())
    }
    
    //println(TableSet)
//    val ppr = new scala.xml.PrettyPrinter(80,5)
//    println("finished parsing")
//    val out = ppr.format(ele)
    //println(out)
    
    //val xmlString = xml.XML.loadFile("database.xml")
  }

}