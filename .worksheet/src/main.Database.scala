package main

object Database {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(80); 
  def c(cmd: String):Unit = ParseExpr.parse(cmd);System.out.println("""c: (cmd: String)Unit""");$skip(44); 
  println("Welcome to the Scala worksheet");$skip(63); 
  ParseExpr.parse("define table b having fields(bb integer);");$skip(39); 
  ParseExpr.parse("insert(5) into b;");$skip(39); 
  ParseExpr.parse("insert(7) into b;");$skip(39); 
  ParseExpr.parse("insert(1) into b;");$skip(40); 
  ParseExpr.parse("insert(24) into b;");$skip(39); 
  ParseExpr.parse("insert(3) into b;");$skip(63); 
  ParseExpr.parse("define table a having fields(aa integer);");$skip(39); 
  ParseExpr.parse("insert(3) into a;");$skip(39); 
  ParseExpr.parse("insert(4) into a;");$skip(39); 
  ParseExpr.parse("insert(7) into a;");$skip(39); 
  ParseExpr.parse("insert(2) into a;");$skip(39); 
  ParseExpr.parse("insert(1) into a;");$skip(19); 
	println(TableSet);$skip(21); 
  Print("a").execute;$skip(21); 
  Print("b").execute;$skip(40); 
  ParseExpr.parse("intersect a and b;");$skip(40); 
  ParseExpr.parse("intersect b and a;");$skip(36); 
  ParseExpr.parse("union a and b;");$skip(36); 
  ParseExpr.parse("minus a and b;");$skip(36); 
  ParseExpr.parse("minus b and a;");$skip(44); 
  ParseExpr.parse("select a where aa < 5;");$skip(56); 
  ParseExpr.parse("minus select a where aa < 9 and b;");$skip(60); 
  
  ParseExpr.parse("select minus a and b where aa < 7;");$skip(30); 
  c("select a where aa < 5;");$skip(45); 
  c("join minus a and b and minus b and a;");$skip(53); 
  c("order select join a and b where aa < 7 by aa;")}

}