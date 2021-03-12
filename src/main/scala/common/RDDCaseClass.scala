package common

trait RDDCaseClass {
  case class SpotifyData(
                          acousticness: Double, artists: String, danceability: Double,
                          duration_ms: Long, energy: Double, explicit: Int, id: String,
                          instrumentalness: Double, key: Int, liveness: Double,
                          loudness: Double, mode: Int, name: String, popularity: Int,
                          release_date: String, speechiness: Double, tempo: Double,
                          valence: Double, year: String) {
    def this(cols: String*) = {
      this(cols(0).toDouble,cols(1),cols(2).toDouble,cols(3).toLong,cols(4).toDouble,
        cols(5).toInt,cols(6),cols(7).toDouble,cols(8).toInt,cols(9).toDouble,cols(10).toDouble,
        cols(11).toInt,cols(12),cols(13).toInt,cols(14),cols(15).toDouble,cols(16).toDouble,
        cols(17).toDouble,cols(18))
    }
  }
  case class GroupAvgDanceability(artist: String, average: Double, count: Long)
}
