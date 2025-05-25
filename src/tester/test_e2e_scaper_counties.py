import unittest
import scraper


class ScraperCountyTests(unittest.TestCase):

    def test_e2e_scraper_anderson(self):
        scraper.Scraper().scrape(
            county="anderson", end_date="2022-03-28", start_date="2022-03-28", judicial_officers=["Calhoon, Mark"],
            ms_wait=0
        )

    def test_e2e_scraper_angelina(self):
        scraper.Scraper().scrape(
            county="angelina", end_date="2022-01-19", start_date="2022-01-19", judicial_officers=["GRUBBS, PATRICIA"],
            ms_wait=0
        )

    def test_e2e_scraper_austin(self):
        scraper.Scraper().scrape(
            county="austin", end_date="2022-04-27", start_date="2022-04-27", judicial_officers=["Burger, Bernice"],
            ms_wait=0
        )

    def test_e2e_scraper_bastrop(self):
        scraper.Scraper().scrape(
            county="bastrop", end_date="2022-04-21", start_date="2022-04-21", judicial_officers=["Allen, Cindy"],
            ms_wait=0
        )

    def test_e2e_scraper_bell(self):
        scraper.Scraper().scrape(
            county="bell", end_date="2022-04-29", start_date="2022-04-29", judicial_officers=["Coleman, Clifford"],
            ms_wait=0
        )

    def test_e2e_scraper_bexar(self):
        scraper.Scraper().scrape(
            county="bexar", end_date="2022-03-07", start_date="2022-03-07", judicial_officers=["ALVARADO, ROSIE"], ms_wait=0
        )

    def test_e2e_scraper_bowie(self):
        scraper.Scraper().scrape(
            county="bowie", end_date="2022-03-07", start_date="2022-03-07", judicial_officers=["Miller, Bill"], ms_wait=0
        )

    def test_e2e_scraper_brazoria(self):
        scraper.Scraper().scrape(
            county="brazoria", end_date="2022-01-10", start_date="2022-01-10", judicial_officers=["Holder, Terri"], ms_wait=0
        )

    def test_e2e_scraper_burnet(self):
        scraper.Scraper().scrape(
            county="burnet", end_date="2019-04-05", start_date="2019-04-05", judicial_officers=["Baardsen, Dawn"], ms_wait=0
        )

    def test_e2e_scraper_calhoun(self):
        scraper.Scraper().scrape(
            county="calhoun", end_date="2022-01-12", start_date="2022-01-12",
            judicial_officers=["Williams, Stephen"], ms_wait=0
        )

    def test_e2e_scraper_cameron(self):
        scraper.Scraper().scrape(
            county="cameron", end_date="2022-02-23", start_date="2022-02-23",
            judicial_officers=["Alejandro, Leonel"], ms_wait=0
        )

    def test_e2e_scraper_chambers(self):
        scraper.Scraper().scrape(
            county="chambers", end_date="2022-03-25", start_date="2022-03-25",
            judicial_officers=["Baker, Jennifer"], ms_wait=0
        )

    def test_e2e_scraper_collin(self):
        scraper.Scraper().scrape(
            county="collin", end_date="2022-03-04", start_date="2022-03-04",
            judicial_officers=["Mason, Corinne A."], ms_wait=0
        )

    def test_e2e_scraper_comal(self):
        scraper.Scraper().scrape(
            county="comal", end_date="2021-01-05", start_date="2021-01-05",
            judicial_officers=["Wigington, Deborah"], ms_wait=0
        )

    def test_e2e_scraper_dallas(self):
        scraper.Scraper().scrape(
            county="dallas", end_date="2022-02-03", start_date="2022-02-03", judicial_officers=["BROWN, MARY"], ms_wait=0
        )

    def test_e2e_scraper_denton(self):
        scraper.Scraper().scrape(
            county="denton", end_date="2022-05-18", start_date="2022-05-18", judicial_officers=["Hughey, Harris"], ms_wait=0
        )

    def test_e2e_scraper_ector(self):
        scraper.Scraper().scrape(
            county="ector", end_date="2022-02-28", start_date="2022-02-28", judicial_officers=["Wilson, Kathy"], ms_wait=0
        )

    def test_e2e_scraper_fannin(self):
        scraper.Scraper().scrape(
            county="fannin", end_date="2022-01-12", start_date="2022-01-12", judicial_officers=["Blake, Laurine"], ms_wait=0
        )

    def test_e2e_scraper_fortbend(self):
        scraper.Scraper().scrape(
            county="fort bend", end_date="2022-05-19", start_date="2022-05-19", judicial_officers=["Wallace, Toni"], ms_wait=0
        )

    def test_e2e_scraper_galveston(self):
        scraper.Scraper().scrape(
            county="galveston", end_date="2022-01-13", start_date="2022-01-13", judicial_officers=["Ellisor, John"], ms_wait=0
        )

    def test_e2e_scraper_gillespie(self):
        scraper.Scraper().scrape(
            county="gillespie", end_date="2022-02-17", start_date="2022-02-17",
            judicial_officers=["McCann, Linda Meier"], ms_wait=0
        )

    def test_e2e_scraper_grayson(self):
        scraper.Scraper().scrape(
            county="grayson", end_date="2022-03-31", start_date="2022-03-31", judicial_officers=["Fallon, Jim"], ms_wait=0
        )

    def test_e2e_scraper_gregg(self):
        scraper.Scraper().scrape(
            county="gregg", end_date="2022-03-03", start_date="2022-03-03", judicial_officers=["Phillips, R. Kent"], ms_wait=0
        )

    def test_e2e_scraper_guadalupe(self):
        scraper.Scraper().scrape(
            county="guadalupe", end_date="2022-02-04", start_date="2022-02-04",
            judicial_officers=["Crawford, Jessica"], ms_wait=0
        )

    def test_e2e_scraper_hale(self):
        scraper.Scraper().scrape(
            county="hale", end_date="2022-01-05", start_date="2022-01-05", judicial_officers=["Davis, Karen"], ms_wait=0
        )

    def test_e2e_scraper_harris(self):
        scraper.Scraper().scrape(
            county="harris", end_date="2022-01-19", start_date="2022-01-19", judicial_officers=["Adams, Wanda"], ms_wait=0
        )

    def test_e2e_scraper_harrison(self):
        scraper.Scraper().scrape(
            county="harrison", end_date="2022-03-29", start_date="2022-03-29", judicial_officers=["Black, Joe"], ms_wait=0
        )

    def test_e2e_scraper_hays(self):
        scraper.Scraper().scrape(
            county="hays", end_date="2022-05-10", start_date="2022-05-10", judicial_officers=["Updegrove, Robert"], ms_wait=0
        )

    def test_e2e_scraper_henderson(self):
        scraper.Scraper().scrape(
            county="henderson", end_date="2022-02-15", start_date="2022-02-15",
            judicial_officers=["Perryman, Nancy"], ms_wait=0
        )

    def test_e2e_scraper_hidalgo(self):
        scraper.Scraper().scrape(
            county="hidalgo", end_date="2022-03-21", start_date="2022-03-21", judicial_officers=["Gonzalez, Noe"], ms_wait=0
        )

    def test_e2e_scraper_howard(self):
        scraper.Scraper().scrape(
            county="howard", end_date="2022-03-24", start_date="2022-03-24", judicial_officers=["Yeats, Timothy D"], ms_wait=0
        )

    def test_e2e_scraper_hunt(self):
        scraper.Scraper().scrape(
            county="hunt", end_date="2022-04-18", start_date="2022-04-18", judicial_officers=["Aiken, Keli M."], ms_wait=0
        )

    def test_e2e_scraper_johnson(self):
        scraper.Scraper().scrape(
            county="johnson", end_date="2022-02-28", start_date="2022-02-28", judicial_officers=["Monk, Jeff"], ms_wait=0
        )

    def test_e2e_scraper_lamar(self):
        scraper.Scraper().scrape(
            county="lamar", end_date="2022-01-25", start_date="2022-01-25", judicial_officers=["Bell, Brandon"], ms_wait=0
        )

    def test_e2e_scraper_liberty(self):
        scraper.Scraper().scrape(
            county="liberty", end_date="2022-01-11", start_date="2022-01-11",
            judicial_officers=["Chambers, Thomas"], ms_wait=0
        )

    def test_e2e_scraper_lubbock(self):
        scraper.Scraper().scrape(
            county="lubbock", end_date="2022-03-03", start_date="2022-03-03", judicial_officers=["Darnell, Kara"], ms_wait=0
        )

    def test_e2e_scraper_matagorda(self):
        scraper.Scraper().scrape(
            county="matagorda", end_date="2022-03-08", start_date="2022-03-08",
            judicial_officers=["Sanders, Jason K."], ms_wait=0
        )

    def test_e2e_scraper_medina(self):
        scraper.Scraper().scrape(
            county="medina", end_date="2022-01-06", start_date="2022-01-06", judicial_officers=["Cashion, Mark"], ms_wait=0
        )

    def test_e2e_scraper_montgomery(self):
        scraper.Scraper().scrape(
            county="montgomery", end_date="2022-02-16", start_date="2022-02-16", judicial_officers=["Grant, Phil"], ms_wait=0
        )

    def test_e2e_scraper_morris(self):
        scraper.Scraper().scrape(
            county="morris", end_date="2022-01-26", start_date="2022-01-26", judicial_officers=["Fridia, Nikita"], ms_wait=0
        )

    def test_e2e_scraper_navarro(self):
        scraper.Scraper().scrape(
            county="navarro", end_date="2022-03-23", start_date="2022-03-23",
            judicial_officers=["LAGOMARSINO, JAMES"], ms_wait=0
        )

    def test_e2e_scraper_nueces(self):
        scraper.Scraper().scrape(
            county="nueces", end_date="2022-01-13", start_date="2022-01-13", judicial_officers=["Galvan, Bobby"], ms_wait=0
        )

    def test_e2e_scraper_panola(self):
        scraper.Scraper().scrape(
            county="panola", end_date="2022-04-11", start_date="2022-04-11", judicial_officers=["Bailey, Terry D"], ms_wait=0
        )

    def test_e2e_scraper_parker(self):
        scraper.Scraper().scrape(
            county="parker", end_date="2022-02-23", start_date="2022-02-23", judicial_officers=["Deen, Pat"], ms_wait=0
        )

    def test_e2e_scraper_potter(self):
        scraper.Scraper().scrape(
            county="potter", end_date="2022-04-07", start_date="2022-04-07", judicial_officers=["Baker, Carry"], ms_wait=0
        )

    def test_e2e_scraper_randall(self):
        scraper.Scraper().scrape(
            county="randall", end_date="2022-01-25", start_date="2022-01-25", judicial_officers=["Anderson, James"], ms_wait=0
        )

    def test_e2e_scraper_rockwall(self):
        scraper.Scraper().scrape(
            county="rockwall", end_date="2022-01-20", start_date="2022-01-20", judicial_officers=["Hall, Brett"], ms_wait=0
        )

    def test_e2e_scraper_sanjacinto(self):
        scraper.Scraper().scrape(
            county="san jacinto", end_date="2022-05-10", start_date="2022-05-10",
            judicial_officers=["Faulkner, Fritz"], ms_wait=0
        )

    def test_e2e_scraper_smith(self):
        scraper.Scraper().scrape(
            county="smith", end_date="2022-02-02", start_date="2022-02-02", judicial_officers=["Ellis, Jason"], ms_wait=0
        )

    def test_e2e_scraper_tarrant(self):
        scraper.Scraper().scrape(
            county="tarrant", end_date="2022-03-02", start_date="2022-03-02", judicial_officers=["McGown, Quentin"], ms_wait=0
        )

    def test_e2e_scraper_taylor(self):
        scraper.Scraper().scrape(
            county="taylor", end_date="2022-04-07", start_date="2022-04-07", judicial_officers=["Harper, Robert"], ms_wait=0
        )

    def test_e2e_scraper_tomgreen(self):
        scraper.Scraper().scrape(
            county="tom green", end_date="2022-04-11", start_date="2022-04-11", judicial_officers=["Nolen, Ben"], ms_wait=0
        )

    def test_e2e_scraper_travis(self):
        scraper.Scraper().scrape(
            county="travis", end_date="2022-04-29", start_date="2022-04-29", judicial_officers=["Holmes, Sylvia"], ms_wait=0
        )

    def test_e2e_scraper_victoria(self):
        scraper.Scraper().scrape(
            county="victoria", end_date="2022-01-31", start_date="2022-01-31",
            judicial_officers=["Ernst, Travis H."], ms_wait=0
        )

    def test_e2e_scraper_walker(self):
        scraper.Scraper().scrape(
            county="walker", end_date="2022-04-12", start_date="2022-04-12", judicial_officers=["Holt, Mark"], ms_wait=0
        )

    def test_e2e_scraper_waller(self):
        scraper.Scraper().scrape(
            county="waller", end_date="2022-03-23", start_date="2022-03-23",
            judicial_officers=["Jackson, Marian E."], ms_wait=0
        )

    def test_e2e_scraper_webb(self):
        scraper.Scraper().scrape(
            county="webb", end_date="2022-05-02", start_date="2022-05-02", judicial_officers=["Villarreal, Victor"], ms_wait=0
        )

    def test_e2e_scraper_wichita(self):
        scraper.Scraper().scrape(
            county="wichita", end_date="2022-01-05", start_date="2022-01-05",
            judicial_officers=["Barnard, Charles"], ms_wait=0
        )

    def test_e2e_scraper_williamson(self):
        scraper.Scraper().scrape(
            county="williamson", end_date="2022-03-22", start_date="2022-03-22",
            judicial_officers=["McLean, Evelyn"], ms_wait=0
        )

    def test_e2e_scraper_wise(self):
        scraper.Scraper().scrape(
            county="wise", end_date="2022-03-15", start_date="2022-03-15", judicial_officers=["Garrett, Willie"], ms_wait=0
        )

    def test_e2e_scraper_wood(self):
        scraper.Scraper().scrape(
            county="wood", end_date="2022-03-10", start_date="2022-03-10", judicial_officers=["McCampbell, J Brad"], ms_wait=0
        )