import groovyx.net.http.HTTPBuilder

import java.math.MathContext
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch

class MomondoParser {

    static def countries = ["US", "RU", "DE", "AU", "CA", "AU", "UK", "FI", "EE", "FR", "IT", "CH", "SE"]

    static def latch = new CountDownLatch(countries.size())

    // search properties
    static def from = "TLV"
    static def to = "FRA"
    static def departureDate = "2099-01-01"
    static def maxDuration = 1200
    static def is2way = true
    static def fromReturn = "FRA"
    static def toReturn = "TLV"
    static def returnDate = "2099-01-01"

    public static void main(String[] args) {

        def result = [] as ConcurrentLinkedQueue
        countries.each { country ->
            Thread.start {
                result.addAll(scan(country))
                latch.countDown()
            }
        }


        def currencies = fetchRates()

        latch.await()

        def sorted = result
                .findAll { it.duration < maxDuration && it.price && it.currency && currencies[it.currency]}
                .collect {
                    if (it.currency != 'EUR') {
                        it.price = (it.price as BigDecimal).divide(currencies[it.currency] as BigDecimal, MathContext.DECIMAL32)
                        it.currency = 'EUR'
                    }
                    it
                }.sort { a, b ->
                    a.price.compareTo(b.price)
                }

        // saving to file
        def resultFileName = "c://temp//momomondo_result_${from}_${to}_${departureDate}_${returnDate}.csv"
        Files.deleteIfExists(Paths.get(resultFileName))
        def file = new File(resultFileName)
        sorted.each { file << it.toCsv() }

        sorted.eachWithIndex { flight, index ->
            if (index < 20) println "Found ticket for $flight.price $flight.currency $flight.legs segments ($flight.duration/$flight.score) in $flight.country at $flight.url"
        }
    }

    static def scan(country) {
        println "Processing $country"

        def url = "http://android.momondo.com"
        def http = new HTTPBuilder(url)
        setProxy(http)

        def headers = ["Content-Type": "application/json"]

        def _2waySearch = is2way ? """
,
    {
      "Origin": "$fromReturn",
      "Destination": "$toReturn",
      "Departure": "$returnDate"
    }""" : ""

        def searchRequest2way = """{
  "Culture": "en-US",
  "Market": "$country",
  "Application": "Android",
  "Consumer": "momondo",
  "Mix": "NONE",
  "Mobile": true,
  "TicketClass": "ECO",
  "AdultCount": 1,
  "ChildAges": [],
  "Segments": [
    {
      "Origin": "$from",
      "Destination": "$to",
      "Departure": "$departureDate"
    }$_2waySearch
  ]
}"""

        def path = "/api/3.0/FlightSearch"
        def flightSearch = http.post(path: path, body: searchRequest2way, headers: headers) { _, reader ->
            "/${reader["SearchId"]}/${reader["EngineId"]}"
        }

        def searchResult = ["Flights":[], "Offers":[], "Segments":[], "Fees":[]]
        def inProcess = true

        // it takes momondo around 20-30 secs to complete the search, also they can ban you for frequent queries
        Thread.sleep(30000)

        while (inProcess) {
            println "Querying country $country"
            http.get(path: path + flightSearch, headers: headers) { _, reader ->
                inProcess = !reader["Done"]

                searchResult["Offers"] += reader["Offers"]
                searchResult["Flights"] += reader["Flights"]
                searchResult["Segments"] += reader["Segments"]
                // TODO add fees

                if (inProcess) { Thread.sleep(10000) }
                if (reader["Error"]) {
                    inProcess = false
                    println "Error ${reader['ErrorMessage']}"
                    return
                }

            }
        }
        // search finished
        def flights = []
        searchResult["Offers"].eachWithIndex { offer, offerIndex ->
            if (!offer) return

            def duration = 0
            def score = offer["Score"]
            def price = offer["TotalPrice"]
            def currency = offer["Currency"]
            def flightIndex = offer["FlightIndex"]
            def flightUrl = offer["Deeplink"]
            def segments = 0

            try {
                // sometimes no certain flight data is returned, so skipping those currupted flights
                searchResult["Flights"][flightIndex]["SegmentIndexes"]?.each { segmentIndex ->
                    duration += searchResult["Segments"][segmentIndex]["Duration"]
                    segments = searchResult["Segments"][segmentIndex]["LegIndexes"]?.size()
                }
            } catch (ignored) {}

            flights << new Flight(price: price, currency: currency, score: score, duration: duration, country: country, url: flightUrl, legs: segments)
        }
        println "Country $country done"
        flights
    }

    static def setProxy(http) {
//        http.setProxy("localhost", 8080, 'http')
    }

    static def fetchRates() {
        def http = new HTTPBuilder("http://android.momondo.com?all=all")
        setProxy(http)
        def result = [:]
        http.get(path: '/api/3.0/Currency/List', query: [all:'all']) { _, reader ->
            reader?.each { rate ->
                result[rate["Code"]] = rate["Rate"]
            }
        }
        result
    }

}


class Flight {
    def country, price, duration = 0, score, currency, url, legs

    def toCsv() {
        "$price,$currency,$country,$duration,$legs,$score,$url\n"
    }
}