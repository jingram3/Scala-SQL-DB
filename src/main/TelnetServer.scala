package main

import akka.actor.{ Actor, ActorRef, IO, IOManager, ActorLogging, Props }
import akka.util.duration._
import akka.util.ByteString
import scala.collection.mutable.Map
import akka.util.Timeout
import akka.pattern.ask

class TelnetServer( db : ActorRef ) extends Actor with ActorLogging {
  import TelnetServer._
  // The 'subservers' stores the map of Actors-to-clients that we need
  // in order to route future communications
  val subservers = Map.empty[IO.Handle, ActorRef]
  // Opens the server's socket and starts listening for incoming stuff
  val serverSocket1 = IOManager( context.system ).listen( "0.0.0.0", 31733 )
  val serverSocket2 = IOManager( context.system ).listen( "0.0.0.0", 29001 )
  val serverSocket3 = IOManager( context.system).listen( "0.0.0.0", 43061)
  def receive = {
    // This message is sent by IO when our server officially starts
    case IO.Listening( server, address ) =>
      log.info( "Telnet Server listening on port {}", address )
    // When a client connects (e.g. telnet) we get this message
    case IO.NewClient( server ) =>
      log.info( "New incoming client connection on server" )
      // You must accept the socket, which can pass to the sub server
      // as well as used as a 'key' into our map to know where future
      // communications come from
      val socket = server.accept()
      socket.write( ByteString( welcome ) )
      subservers += ( socket ->
        context.actorOf( Props( new SubServer( socket, db ) ) ) )
    // Every time we get a message it comes in as a ByteString on
    // this message
    case IO.Read( socket, bytes ) if subservers.contains(socket) =>
      // Convert from ByteString to ascii (helper from companion)
      val cmd = ascii( bytes )
      // Send the message to the subserver, looked up by socket
      subservers( socket ) ! NewMessage( cmd )
    // Client closed connection, kill the sub server
    case IO.Close( socket ) =>
      log.info("closing socket: " + socket)
      socket.close
      context.stop( subservers( socket ) )
      subservers -= socket
  }
}
object TelnetServer {

  import Database._
  // For the upcoming ask calls
  implicit val askTimeout = Timeout( 1.second )
  // The welcome message was sent on connection
  val welcome =
    """|Welcome to database!
|----------------
| Feel free to change the login screen anyway you'd like
| You could put instructions here
|
|> """.stripMargin
  // Simple method to convert from ByteString messages to
  // the Strings we know we're going to get
  def ascii( bytes : ByteString ) : String = {
    bytes.decodeString( "UTF-8" ).trim
  }
  // To ease the SubServer's implementation we will send it Strings
  // instead of ByteStrings that it would need to decode anyway
  case class NewMessage( msg : String )
  case class Response( msg : String )
  // The SubServer. We give it the socket that it can use for giving
  // replies back to the telnet client and the database to which it will
  // ask questions to get status.  Not sure you really need this (you can hook up directly to 
  // the database) but thought I'd throw this in anyway.
  class SubServer( socket : IO.SocketHandle, db : ActorRef ) extends Actor {
    // here is an example of a possible response :)
    def headStr : ByteString =
      ByteString( "heading: \n" )

    // Here is how we might notify the user
    def handleHeading() = {
      socket.write( headStr )
    }
    // Receive NewMessages and deal with them
    def receive = {
      case NewMessage( msg ) =>
        if ( msg == "exit;" ) {
          socket.write( ByteString( "Socket closing: " + socket ) )
          sender ! IO.Close( socket)
        } else {
          handleHeading()
          db ! Message( msg )
        }
      case Response( msg ) =>
        socket.write( ByteString( msg + "\n" ) )
        
      socket.write(ByteString(">"))
    }
  }
}
