package rajkumar.org.utils

import java.util._

import scala.collection.JavaConversions._

// Given year since 1900, return the java.util.Date of holiday and
// observed holiday (if holiday fell on weekend)
// Also given date, tells us if it was a major-holiday
// Eg:
// val year = new Date( System.currentTImeMillis ).getYear
// val date = NewYearsDay( year )
// isMajorHoliday( date )

object Holidays {

  def NewYearsDayObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 0
    val nMonthDecember = 11
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 1)
    nX = dtD.getDay
    val mYear = if (nYear > 1900)  nYear - 1900 else nYear

    nX match {
      case 0 => new Date(mYear, nMonth, 2)
      case 1 | 2 | 3 | 4 | 5 => new Date(mYear, nMonth, 1)
      case _ => new Date(mYear, nMonthDecember, 31)
    }
  }

  def NewYearsDay(nYear: Int): Date = {
    val nMonth = 0
    new Date(nYear, nMonth, 1)
  }

  def RobertELeeDay(nYear: Int): Date = {
    val nMonth = 0
    new Date(nYear, nMonth, 18)
  }

  def GroundhogDay(nYear: Int): Date = {
    val nMonth = 1
    new Date(nYear, nMonth, 8)
  }

  def AbrahamLincolnsBirthday(nYear: Int): Date = {
    val nMonth = 1
    new Date(nYear, nMonth, 12)
  }

  def ValentinesDay(nYear: Int): Date = {
    val nMonth = 1
    new Date(nYear, nMonth, 14)
  }

  def SusanBAnthonyDay(nYear: Int): Date = {
    val nMonth = 1
    new Date(nYear, nMonth, 15)
  }

  def PresidentsDayObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 1
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 1)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 16)
      case 1 => new Date(nYear, nMonth, 15)
      case 2 => new Date(nYear, nMonth, 21)
      case 3 => new Date(nYear, nMonth, 20)
      case 4 => new Date(nYear, nMonth, 19)
      case 5 => new Date(nYear, nMonth, 18)
      case _ => new Date(nYear, nMonth, 17)
    }
  }

  def SaintPatricksDay(nYear: Int): Date = {
    val nMonth = 2
    new Date(nYear, nMonth, 17)
  }

  def GoodFridayObserved(nYear: Int): Date = {
    var nEasterMonth = 0
    var nEasterDay = 0
    var nGoodFridayMonth = 0
    var nGoodFridayDay = 0
    var dEasterSunday: Date = null
    dEasterSunday = EasterSunday(nYear)
    nEasterMonth = dEasterSunday.getMonth
    nEasterDay = dEasterSunday.getDate
    if (nEasterDay <= 3 && nEasterMonth == 3) nEasterDay match {
      case 3 => 
      nGoodFridayMonth = nEasterMonth - 1
      nGoodFridayDay = nEasterDay - 2

      case 2 => 
      nGoodFridayMonth = nEasterMonth - 1
      nGoodFridayDay = 31

      case 1 => 
      nGoodFridayMonth = nEasterMonth - 1
      nGoodFridayDay = 31

      case _ => 
      nGoodFridayMonth = nEasterMonth
      nGoodFridayDay = nEasterDay - 2

    } else {
      nGoodFridayMonth = nEasterMonth
      nGoodFridayDay = nEasterDay - 2
    }
    new Date(nYear, nGoodFridayMonth, nGoodFridayDay)
  }

  def EasterSunday(uYear: Int): Date = {
    var nA = 0
    var nB = 0
    var nC = 0
    var nD = 0
    var nE = 0
    var nF = 0
    var nG = 0
    var nH = 0
    var nI = 0
    var nK = 0
    var nL = 0
    var nM = 0
    var nP = 0
    var nYY = 0
    var nEasterMonth = 0
    var nEasterDay = 0
    nYY = uYear
    val nYear = if (uYear < 1900) uYear + 1900 else uYear
    
    nA = nYear % 19
    nB = nYear / 100
    nC = nYear % 100
    nD = nB / 4
    nE = nB % 4
    nF = (nB + 8) / 25
    nG = (nB - nF + 1) / 3
    nH = (19 * nA + nB - nD - nG + 15) % 30
    nI = nC / 4
    nK = nC % 4
    nL = (32 + 2 * nE + 2 * nI - nH - nK) % 7
    nM = (nA + 11 * nH + 22 * nL) / 451
    nEasterMonth = (nH + nL - 7 * nM + 114) / 31
    nEasterMonth
    nP = (nH + nL - 7 * nM + 114) % 31
    nEasterDay = nP + 1
    val mYear = nYear - 1900
    new Date(mYear, nEasterMonth, nEasterDay)
  }

  def EasterMonday(nYear: Int): Date = {
    var nEasterMonth = 0
    var nEasterDay = 0
    val nMonthMarch = 2
    val nMonthApril = 3
    val dEasterSunday = EasterSunday(nYear)
    nEasterMonth = dEasterSunday.getMonth
    nEasterDay = dEasterSunday.getDay
    if (nEasterMonth == nMonthMarch || nEasterDay == 31) {
      new Date(nYear, nMonthApril, 1)
    } else {
      new Date(nYear, nEasterMonth, nEasterDay)
    }
  }

  def CincoDeMayo(nYear: Int): Date = {
    val nMonth = 4
    new Date(nYear, nMonth, 5)
  }

  def MemorialDayObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 4
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 31)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 25)
      case 1 => new Date(nYear, nMonth, 31)
      case 2 => new Date(nYear, nMonth, 30)
      case 3 => new Date(nYear, nMonth, 29)
      case 4 => new Date(nYear, nMonth, 28)
      case 5 => new Date(nYear, nMonth, 27)
      case _ => new Date(nYear, nMonth, 26)
    }
  }

  def IndependenceDayObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 6
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 4)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 5)
      case 1 | 2 | 3 | 4 | 5 => new Date(nYear, nMonth, 4)
      case _ => new Date(nYear, nMonth, 3)
    }
  }

  def IndependenceDay(nYear: Int): Date = {
    val nMonth = 6
    new Date(nYear, nMonth, 4)
  }

  def CanadianCivicHoliday(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 7
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 1)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 2)
      case 1 => new Date(nYear, nMonth, 1)
      case 2 => new Date(nYear, nMonth, 7)
      case 3 => new Date(nYear, nMonth, 6)
      case 4 => new Date(nYear, nMonth, 5)
      case 5 => new Date(nYear, nMonth, 4)
      case _ => new Date(nYear, nMonth, 3)
    }
  }

  def LaborDayObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 8
    var dtD: Date = null
    dtD = new Date(nYear, 9, 1)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 2)
      case 1 => new Date(nYear, nMonth, 7)
      case 2 => new Date(nYear, nMonth, 6)
      case 3 => new Date(nYear, nMonth, 5)
      case 4 => new Date(nYear, nMonth, 4)
      case 5 => new Date(nYear, nMonth, 3)
      case _ => new Date(nYear, nMonth, 2)
    }
  }

  def ColumbusDayObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 9
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 1)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 9)
      case 1 => new Date(nYear, nMonth, 15)
      case 2 => new Date(nYear, nMonth, 14)
      case 3 => new Date(nYear, nMonth, 13)
      case 4 => new Date(nYear, nMonth, 12)
      case 5 => new Date(nYear, nMonth, 11)
      case _ => new Date(nYear, nMonth, 10)
    }
  }

  def Halloween(nYear: Int): Date = {
    val nMonth = 9
    (new Date(nYear, nMonth, 31))
  }

  def USElectionDay(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 10
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 1)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 3)
      case 1 => new Date(nYear, nMonth, 2)
      case 2 => new Date(nYear, nMonth, 1)
      case 3 => new Date(nYear, nMonth, 7)
      case 4 => new Date(nYear, nMonth, 6)
      case 5 => new Date(nYear, nMonth, 5)
      case _ => new Date(nYear, nMonth, 4)
    }
  }

  def VeteransDayObserved(nYear: Int): Date = {
    val nMonth = 10
    new Date(nYear, nMonth, 11)
  }

  def RememberenceDayObserved(nYear: Int): Date = VeteransDayObserved(nYear)

  def ThanksgivingObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 10
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 1)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 26)
      case 1 => new Date(nYear, nMonth, 25)
      case 2 => new Date(nYear, nMonth, 24)
      case 3 => new Date(nYear, nMonth, 23)
      case 4 => new Date(nYear, nMonth, 22)
      case 5 => new Date(nYear, nMonth, 28)
      case _ => new Date(nYear, nMonth, 27)
    }
  }

  def ChristmasDayObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 11
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 25)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 26)
      case 1 | 2 | 3 | 4 | 5 => new Date(nYear, nMonth, 25)
      case _ => new Date(nYear, nMonth, 24)
    }
  }

  def ChristmasDay(nYear: Int): Date = {
    val nMonth = 11
    new Date(nYear, nMonth, 25)
  }

  def MartinLutherKingObserved(nYear: Int): Date = {
    var nX: Int = 0
    val nMonth = 0
    var dtD: Date = null
    dtD = new Date(nYear, nMonth, 1)
    nX = dtD.getDay
    nX match {
      case 0 => new Date(nYear, nMonth, 16)
      case 1 => new Date(nYear, nMonth, 15)
      case 2 => new Date(nYear, nMonth, 21)
      case 3 => new Date(nYear, nMonth, 20)
      case 4 => new Date(nYear, nMonth, 19)
      case 5 => new Date(nYear, nMonth, 18)
      case _ => new Date(nYear, nMonth, 17)
    }
  }

  // - Some Utilities -----
  val MajorHolidays = Seq[(Int) => Date](
    Holidays.NewYearsDay(_),
    Holidays.PresidentsDayObserved(_),
    Holidays.GoodFridayObserved(_),
    Holidays.EasterSunday(_),
    Holidays.MemorialDayObserved(_),
    Holidays.IndependenceDayObserved(_),
    Holidays.IndependenceDay(_),
    Holidays.LaborDayObserved(_),
    Holidays.Halloween(_),
    Holidays.ThanksgivingObserved(_),
    Holidays.ChristmasDayObserved(_),
    Holidays.ChristmasDay(_)
  )

  def isMajorHoliday( d: Date ):Boolean = {
    val year = d.getYear
    val dates = MajorHolidays.map( f  => f( year ))
    dates.exists( _ == d )
  }

  // - Testing --
  def main( args: Array[String]):Unit = {

    println( ChristmasDay( 114 ))
    println( ChristmasDayObserved( 114 ))

    val nyd = new Date( 114, 0, 1 )
    val today = new Date()
    println( isMajorHoliday( today ) )
    println( isMajorHoliday( nyd ) )
  }
}
