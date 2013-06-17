/**
 * Copyright 2011-2013 eBusiness Information, Groupe Excilys (www.ebusinessinformation.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gatling.http.ahc

import scala.collection.JavaConversions.asScalaBuffer
import scala.concurrent.duration.DurationInt

import com.ning.http.client.{ AsyncHttpClient, FluentStringsMap, Request, RequestBuilder }
import com.ning.http.util.AsyncHttpProviderUtils

import akka.actor.{ ActorRef, ReceiveTimeout }
import io.gatling.core.action.BaseActor
import io.gatling.core.check.Checks
import io.gatling.core.config.GatlingConfiguration.configuration
import io.gatling.core.result.message.{ KO, OK, RequestMessage, Status }
import io.gatling.core.result.writer.DataWriter
import io.gatling.core.session.{ Session, SessionPrivateAttributes }
import io.gatling.core.util.StringHelper.eol
import io.gatling.core.util.TimeHelper.nowMillis
import io.gatling.core.validation.{ Failure, Success }
import io.gatling.http.Headers.{ Names => HeaderNames }
import io.gatling.http.cache.CacheHandling
import io.gatling.http.check.HttpCheck
import io.gatling.http.config.HttpProtocol
import io.gatling.http.cookie.CookieHandling
import io.gatling.http.response.{ Response, ResponseBuilder, ResponseBuilderFactory, ResponseProcessor }
import io.gatling.http.util.HttpStringBuilder

object AsyncHandlerActor {
	val redirectedRequestNamePattern = """(.+?) Redirect (\d+)""".r
	val timeout = configuration.http.ahc.requestTimeOutInMs milliseconds

	val httpActorAttributeName = SessionPrivateAttributes.privateAttributePrefix + "http.actor"
}

class AsyncHandlerActor(protocol: HttpProtocol) extends BaseActor {

	// per request state
	var state: AsyncHandlerActorState = _
	var done = false
	var progress: Progress = _
	var expectedProgress: Progress = _
	var responseBuilder: ResponseBuilder = _

	// lazy state
	var client: AsyncHttpClient = _

	override def preStart {
		context.setReceiveTimeout(AsyncHandlerActor.timeout)
	}

	def maybeComplete = if (expectedProgress != null && expectedProgress == progress) {
		done = true
		processResponse(responseBuilder.build)
	}

	def receive = {
		case state @ AsyncHandlerActorState(session, _, _, _, _, _, _) =>
			if (client == null)
				client = if (protocol.shareClient) HttpClient.default else HttpClient.newClient(session)
			sendHttpRequest(state)

		case OnHeaderWriteCompleted(nanos) if !done =>
			progress = progress.copy(headerWriteCompleted = true)
			responseBuilder.updateRequestSendingEndDate(nanos)
			maybeComplete

		case OnContentWriteCompleted(nanos) if !done =>
			progress = progress.copy(contentWriteCompleted = true)
			responseBuilder.updateRequestSendingEndDate(nanos)
			maybeComplete

		case OnStatusReceived(status, nanos) if !done =>
			progress = progress.copy(statusReceived = true)
			responseBuilder.updateResponseReceivingStartDate(nanos)
			responseBuilder.accumulate(status)
			maybeComplete

		case OnHeadersReceived(headers) if !done =>
			progress = progress.copy(headersReceived = true)
			responseBuilder.accumulate(headers)
			maybeComplete

		case OnBodyPartReceived(bodyPart) if !done =>
			progress = progress.copy(bodyPartReceived = progress.bodyPartReceived + 1)
			responseBuilder.accumulate(bodyPart)
			maybeComplete

		case OnCompleted(progress, nanos) if !done =>
			this.expectedProgress = progress
			this.progress = progress.copy(complete = true)
			responseBuilder.computeExecutionEndDateFromNanos(nanos)
			maybeComplete

		case OnThrowable(errorMessage, nanos) if !done =>
			done = true
			ko(state.session, responseBuilder.computeExecutionEndDateFromNanos(nanos).build, errorMessage)

		case ReceiveTimeout if !done =>
			done = true
			ko(state.session, responseBuilder.build, "GatlingAsyncHandlerActor timed out")

		case message if done =>
			logger.warn(s"Received $message while request is done, WTH!!!")
	}

	private def sendHttpRequest(state: AsyncHandlerActorState) {
		this.state = state
		done = false
		progress = new Progress
		responseBuilder = state.responseBuilderFactory(state.request)
		val ahcHandler = state.handlerFactory(state.requestName, self)
		client.executeRequest(state.request, ahcHandler)
	}

	private def logRequest(
		session: Session,
		status: Status,
		response: Response,
		errorMessage: Option[String] = None) {

		def dump = {
			val buff = new StringBuilder
			buff.append(eol).append(">>>>>>>>>>>>>>>>>>>>>>>>>>").append(eol)
			buff.append("Request:").append(eol).append(s"${state.requestName}: $status ${errorMessage.getOrElse("")}").append(eol)
			buff.append("=========================").append(eol)
			buff.append("Session:").append(eol).append(session).append(eol)
			buff.append("=========================").append(eol)
			buff.append("HTTP request:").append(eol).appendAHCRequest(state.request)
			buff.append("=========================").append(eol)
			buff.append("HTTP response:").append(eol).appendResponse(response).append(eol)
			buff.append("<<<<<<<<<<<<<<<<<<<<<<<<<")
			buff.toString
		}

		/**
		 * Extract extra info from both request and response.
		 *
		 * @param response is the response to extract data from; request is retrieved from the property
		 * @return the extracted Strings
		 */
		def extraInfo: List[Any] =
			try {
				protocol.extraInfoExtractor.map(_(status, session, state.request, response)).getOrElse(Nil)
			} catch {
				case e: Exception =>
					logger.warn("Encountered error while extracting extra request info", e)
					Nil
			}

		if (status == KO) {
			logger.warn(s"Request '${state.requestName}' failed : ${errorMessage.getOrElse("")}")
			if (!logger.underlying.isTraceEnabled) logger.debug(dump)
		}
		logger.trace(dump)

		DataWriter.tell(RequestMessage(session.scenarioName, session.userId, session.groupStack, state.requestName,
			response.executionStartDate, response.requestSendingEndDate, response.responseReceivingStartDate, response.executionEndDate,
			status, errorMessage, extraInfo))
	}

	/**
	 * This method is used to send a message to the data writer actor and then execute the next action
	 *
	 * @param newSession the new Session
	 */
	private def executeNext(newSession: Session, response: Response) {
		state.next ! newSession.increaseTimeShift(nowMillis - response.executionEndDate)

		// clean mutable state
		state = null
		expectedProgress = null
		progress = null
		responseBuilder = null
	}

	private def ok(session: Session, response: Response) {
		logRequest(session, OK, response, None)
		executeNext(session, response)
	}

	private def ko(session: Session, response: Response, message: String) {
		val failedSession = session.markAsFailed
		logRequest(failedSession, KO, response, Some(message))
		executeNext(failedSession, response)
	}

	/**
	 * This method processes the response if needed for each checks given by the user
	 */
	private def processResponse(response: Response) {

		def redirect(sessionWithUpdatedCookies: Session) {

			logRequest(sessionWithUpdatedCookies, OK, response)

			val redirectURI = AsyncHttpProviderUtils.getRedirectUri(state.request.getURI, response.getHeader(HeaderNames.LOCATION))

			val requestBuilder = new RequestBuilder(state.request)
				.setMethod("GET")
				.setBodyEncoding(configuration.core.encoding)
				.setQueryParameters(null.asInstanceOf[FluentStringsMap])
				.setParameters(null.asInstanceOf[FluentStringsMap])
				.setUrl(redirectURI.toString)
				.setConnectionPoolKeyStrategy(state.request.getConnectionPoolKeyStrategy)

			for (cookie <- CookieHandling.getStoredCookies(sessionWithUpdatedCookies, redirectURI))
				requestBuilder.addOrReplaceCookie(cookie)

			val newRequest = requestBuilder.build
			newRequest.getHeaders.remove(HeaderNames.CONTENT_LENGTH)
			newRequest.getHeaders.remove(HeaderNames.CONTENT_TYPE)

			val newRequestName = state.requestName match {
				case AsyncHandlerActor.redirectedRequestNamePattern(requestBaseName, redirectCount) => requestBaseName + " Redirect " + (redirectCount.toInt + 1)
				case _ => state.requestName + " Redirect 1"
			}

			sendHttpRequest(AsyncHandlerActorState(sessionWithUpdatedCookies, newRequest, newRequestName, state.checks, state.handlerFactory, state.responseBuilderFactory, state.next))
		}

		def checkAndProceed(sessionWithUpdatedCookies: Session) {
			val sessionWithUpdatedCache = CacheHandling.cache(protocol, sessionWithUpdatedCookies, state.request, response)
			val checkResult = Checks.check(response, sessionWithUpdatedCache, state.checks)

			checkResult match {
				case Success(newSession) => ok(newSession, response)
				case Failure(errorMessage) => ko(sessionWithUpdatedCache, response, errorMessage)
			}
		}

		val sessionWithUpdatedCookies = CookieHandling.storeCookies(state.session, response.getUri, response.getCookies.toList)

		if (response.isRedirected && protocol.followRedirect)
			redirect(sessionWithUpdatedCookies)
		else
			checkAndProceed(sessionWithUpdatedCookies)
	}
}