package main

object Database {
  def c(cmd: String):Unit = ParseExpr.parse(cmd)
  println("Welcome to the Scala worksheet")
  ParseExpr.parse("define table b having fields(bb integer);")  
  ParseExpr.parse("insert(5) into b;")
  ParseExpr.parse("insert(7) into b;")
  ParseExpr.parse("insert(1) into b;")
  ParseExpr.parse("insert(24) into b;")
  ParseExpr.parse("insert(3) into b;")
  ParseExpr.parse("define table a having fields(aa integer);")
  ParseExpr.parse("insert(3) into a;")
  ParseExpr.parse("insert(4) into a;")
  ParseExpr.parse("insert(7) into a;") 
  ParseExpr.parse("insert(2) into a;")
  ParseExpr.parse("insert(1) into a;")
	println(TableSet)
  Print("a").execute
  Print("b").execute
  ParseExpr.parse("intersect a and b;")
  ParseExpr.parse("intersect b and a;")
  ParseExpr.parse("union a and b;")
  ParseExpr.parse("minus a and b;")
  ParseExpr.parse("minus b and a;")
  ParseExpr.parse("select a where aa < 5;")
  ParseExpr.parse("minus select a where aa < 9 and b;")
  
  ParseExpr.parse("select minus a and b where aa < 7;")
  c("select a where aa < 5;")
  c("join minus a and b and minus b and a;")
  c("order select join a and b where aa < 7 by aa;")

}