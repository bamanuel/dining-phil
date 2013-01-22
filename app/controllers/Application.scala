package controllers

import play.api._
import play.api.mvc._
import play.api.libs.concurrent._
import play.api.Play.current
import play.api.data._
import play.api.data.Forms._
import play.api.libs.json
import play.api.libs.iteratee._
import models._
import play.api.libs.Comet

import akka.dispatch.Await
import akka.dispatch.Future
import java.util.concurrent.TimeUnit
import akka.util._
import akka.util.duration._


object Application extends Controller {
  
	def index = Action {
		//val data = Dining.runDinings
		val dataContent:Enumerator[String] = Enumerator.fromCallback(data._2, () => println("Iterator done"))
		//for(i <- dataContent){println(i)}
		//Iteratee.foreach[Array[Byte]](msg => println(new String(msg)))

		//Ok.stream(dataContent.andThen(Enumerator.eof) &> Comet(callback = "console.log"))
		Ok.stream(dataContent)
		//Ok("test")
	}
	
	def start = Action {
		Dining.runDinings
		Ok("Start")
	}
	
	def stop = Action {
		Dining.stopDining
		Ok("End")
	}
	val data = Dining.runDinings
	def act = Action { implicit request =>
		val timeout = Timeout(10 seconds)
		val result = Await.result(data._1.mapTo[List[String]], timeout.duration)//.asInstanceOf[Strings]
		//println(result) 

		// to get a Blah object from request content
		val blah = json.Json.toJson(result).as[List[String]]
	
		// to return Blah as application/json, you just have to convert your Blah to a JsValue and give it to Ok()
		Ok(json.Json.toJson(blah))
	}

}
