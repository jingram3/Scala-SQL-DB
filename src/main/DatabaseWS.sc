package main

object DatabaseWS {
  def c(cmd: String):Unit = ParseExpr.parse(cmd)  //> c: (cmd: String)Unit
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  ParseExpr.parse("print dictionary;")            //> Valid command.
  ParseExpr.parse("define table b having fields(bb integer);")
                                                  //> Valid command.
  ParseExpr.parse("insert(5) into b;")            //> Valid command.
  ParseExpr.parse("insert(7) into b;")            //> Valid command.
  ParseExpr.parse("insert(1) into b;")            //> Valid command.
  ParseExpr.parse("insert(24) into b;")           //> Valid command.
  ParseExpr.parse("insert(3) into b;")            //> Valid command.
  ParseExpr.parse("define table a having fields(aa integer);")
                                                  //> Valid command.
  ParseExpr.parse("insert(3) into a;")            //> Valid command.
  ParseExpr.parse("insert(4) into a;")            //> Valid command.
  ParseExpr.parse("insert(7) into a;")            //> Valid command.
  ParseExpr.parse("insert(2) into a;")            //> Valid command.
  ParseExpr.parse("insert(1) into a;")            //> Valid command.
	println(TableSet)                         //> 
                                                  //| >>>>>>>>>>>>>
  Print("a").execute                              //> aa
                                                  //| 3
                                                  //| 4
                                                  //| 7
                                                  //| 2
                                                  //| 1
  Print("b").execute                              //> bb
                                                  //| 5
                                                  //| 7
                                                  //| 1
                                                  //| 24
                                                  //| 3
  ParseExpr.parse("intersect a and b;")           //> Invalid command.
                                                  //| Failure.
  ParseExpr.parse("intersect b and a;")           //> Invalid command.
                                                  //| Failure.
  ParseExpr.parse("union a and b;")               //> Invalid command.
                                                  //| Failure.
  ParseExpr.parse("minus a and b;")               //> Invalid command.
                                                  //| Failure.
  ParseExpr.parse("minus b and a;")               //> Invalid command.
                                                  //| Failure.
  ParseExpr.parse("select a where aa < 5;")       //> Valid command.
  ParseExpr.parse("minus select a where aa < 9 and b;")
                                                  //> Invalid command.
                                                  //| Failure.
  
  ParseExpr.parse("select minus a and b where aa < 7;")
                                                  //> Invalid command.
                                                  //| Failure.
  c("select a where aa < 5;")                     //> Valid command.
  c("join minus a and b and minus b and a;")      //> Invalid command.
                                                  //| Failure.
  c("order select join a and b where aa < 7 by aa;")
                                                  //> Invalid command.
                                                  //| Failure.\

}