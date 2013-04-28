package main

import java.text.SimpleDateFormat
import java.util.Date

class Table(private val name: String, private val fields: List[Field], private val rows: List[Row]) {
  def getName(): String = name
  
  override
  def toString(): String = {
    name + "\n" + fields.map(x => x.getName + " " + x).mkString("\n")
  }
  
  def printTable(): Unit = {
    println(fields.map(x => x.getName).mkString("\t") + "\n" + rows.mkString("\n"))
  }
  
  def insert(values: List[Value]): Table = {
    new Table(name, fields, rows ::: List(new Row(values, fields)))
  }
  
  def update(fieldName: String, value: Value, where: Option[WhereClause]): Table = {
    new Table(name, fields, rows.map(row => row.update(fieldName, value, where)))
  }
  
  def delete(where: Option[WhereClause]): Table = {
    new Table(name, fields, rows.map(row => row.delete(where)).filterNot(x=>x.isEmpty))
  }
  
  def select(where: Option[WhereClause]): Table = {
    new Table(name, fields, rows.filter(x => x.satisfiesWhere(where)))
  }
  
  def project(field_list: List[String]): Table = {
    new Table(name, fields.filter(x => field_list.contains(x.getName)), rows.map(row => row.over(field_list)))
  }
  
  def join(t: Table): Table = {
    new Table(name, fields ::: t.fields, rows.map(x => t.rows.map(y => x.join(y))).flatten) //rows.zip(t.rows).map(x => x._1.join(x._2)))
  }
  
  def intersect(t: Table): Table = {
    if(unionCompatible(t)) {
      new Table(name, fields, rows.filter(x => t.rows.exists(y => y.values == x.values)))
    }
    else new Table("Not union compatible", Nil, Nil)
  }
  
  def union(t: Table): Table = {
    if(unionCompatible(t)){
      val unioned = rows ::: t.rows
      new Table(name, fields, unique(unioned))
    }
    else new Table("Not union compatible", Nil, Nil)
  }
  
  def minus(t: Table): Table = {
    if(unionCompatible(t)){
      new Table(name, fields, rows.filterNot(x => t.rows.exists(y => y.values == x.values)))
    }
    else new Table("Not union compatible", Nil, Nil)
  }
  
  def sort(field: String): Table = {
    new Table(name, fields, rows.sortWith(_.compareTo(_, field) < 0))
  }
  
  private def unique(rs: List[Row]): List[Row] = {
    def uniq(r: List[Row], acc: List[Row]): List[Row] = {
      if(r.isEmpty) acc
      else if (acc.exists(x => x.values == r.head.values)) uniq(r.tail, acc)
      else uniq(r.tail, r.head :: acc)
    } 
    uniq(rs, List()).reverse
  }
  
  private def unionCompatible(t: Table): Boolean = {
    fields.length == t.fields.length && !(fields.zip(t.fields)).exists(x => x._1.toString!=x._2.toString)
  }
  
  def toXML: scala.xml.Node = {
    <table>
	  <tablename>
	  	{name}
	  </tablename>
	  {for (field <- fields) yield {
		  <field>
	  		<fieldname>
		  		{field.getName}
	  		</fieldname>
	  		<fieldtype>
		  		{field.toString}
	  		</fieldtype>
		  </field>
	  }}
	  <values>
	  	{for (row <- rows) yield {
	  	  {row.toXML}
	  	}}
	  </values> 
	</table>
  }
  
}

object FieldFactory {
  def initField(fType: String, name: String): Field = {
    fType match {
      case "integer" => new IntField(name)
      case "date" => new DateField(name)
      case "real" => new RealField(name)
      case "varchar" => new VarField(name)
      case "boolean" => new BoolField(name)
      //case _ => Nil
    }
  }
}

class Row(val values: List[Value], private val fields: List[Field]) {
  
  val isEmpty: Boolean = values.isEmpty
  
  private val zipped = fields.zip(values)
  
  def update(fieldName: String, value: Value, where: Option[WhereClause]): Row = {
    new Row(zipped.map(pair => if(pair._1.getName == fieldName && satisfiesWhere(where)) value else pair._2), fields)
  }
  
  def delete(where: Option[WhereClause]): Row = {
    if(satisfiesWhere(where)) new Row(Nil, Nil)
    else this
  }
  
  def satisfiesWhere(where: Option[WhereClause]): Boolean = {
    where match {
      case Some(x) => zipped.exists(pair => x.compareTo(pair))
      case None => true
    }
  }
  
  override def equals(that: Any): Boolean = {
    that match {
      case that: Row => this.values == that.values
      case _ => false
    }
  }
  
  def over(field_list: List[String]): Row = {
    val params = zipped.filter(x => field_list.contains(x._1.getName)).unzip
    new Row(params._2, params._1)
  }
  
  def join(row: Row): Row = { //your boat
    new Row(values ::: row.values, fields ::: row.fields)
  }
  
  def compareTo(that: Row, field: String): Int = {
    val thisVal = this.zipped.find(x => x._1.getName == field).get._2
    val thatVal = that.zipped.find(x => x._1.getName == field).get._2
    thisVal.compareTo(thatVal)
  }
  
  override val toString: String = values.mkString("\t")
  
  def toXML: scala.xml.Node = {
    <row>
		{for (pair <- zipped) yield {
		  <column name={pair._1.getName}>
			{pair._2.toString}
		  </column>
		}}
	</row>
  }
}

abstract class Field(private val name: String) {
  def getName: String
  def getValue(value: String): Value
}

class IntField(private val name: String) extends Field(name) {
  def getName: String = name
   def getValue(value: String): Value = new IntValue(value.toInt)
  override val toString: String = "integer"
}

class DateField(private val name: String) extends Field(name) {
  def getName: String = name
  def getValue(value: String): Value = new DateValue(new SimpleDateFormat("MM/dd/yyyy").parse(value.toString))
  override val toString: String = "date"
}

class RealField(private val name: String) extends Field(name) {
  def getName: String = name
  def getValue(value: String): Value = new RealValue(value.toDouble)
  override val toString: String = "real"
}

class VarField(private val name: String) extends Field(name) {
  def getName: String = name
  def getValue(value: String): Value = new VarValue(value.toString)
  override val toString: String = "varchar"
}

class BoolField(private val name: String) extends Field(name) {
  def getName: String = name
  def getValue(value: String): Value = new BoolValue(value.toBoolean)
  override val toString: String = "boolean"
}

abstract class Value(private val value: Any) {
  def getValue: Any = value
  
  def compareTo(that: Value): Int
  
  override val toString: String = value.toString
  
}
case class IntValue(private val value: Int) extends Value {
  override def getValue: Int = value
  override val toString: String = value.toString
   
  override def compareTo(that: Value): Int = that match {case IntValue(x) => value.compareTo(x)}
}
case class DateValue(private val value: Date) extends Value {
  override def getValue: Date = value
  override val toString: String = new SimpleDateFormat("MM/dd/yyyy").format(value)
  
  def compareTo(that: Value): Int = that match {case DateValue(x) => value.compareTo(x)}
}
case class RealValue(private val value: Double) extends Value {
  override def getValue: Double = value
  override val toString: String = value.toString
  
  def compareTo(that: Value): Int = that match {case RealValue(x) => value.compareTo(x)}
}
case class VarValue(private val value: String) extends Value {
  override def getValue: String = value
  override val toString: String = value.toString
  
  def compareTo(that: Value): Int = that match {case VarValue(x) => value.compareTo(x)}
}
case class BoolValue(private val value: Boolean) extends Value {
  override def getValue: Boolean = value
  override val toString: String = value.toString
  
  def compareTo(that: Value): Int = that match {case BoolValue(x) => value.compareTo(x)}
}