import scrapy
from allocinescraper.items import AllocineSeriescraperItem


class AlloSerieSpider(scrapy.Spider):
    name = "allo_serie_spider"
    allowed_domains = ["allocine.fr"]
    # start_urls = [f"https://allocine.fr/series-tv/?page={i}" for i in range(1379)]
    start_urls = ["https://allocine.fr/series-tv/?page=1"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'allocinescraper.pipelines.AllocineSeriescraperPipeline': 400
        }
    }

    def parse(self, response):
        series = response.xpath("//a[@class='meta-title-link']")
       
        for serie in series:
            serie_url = serie.xpath("./@href").get()
            yield response.follow(serie_url, callback=self.parse_serie)

    def parse_serie(self, response):
        item = AllocineSeriescraperItem()
        item["title"] = response.xpath("//div[@class='titlebar-title titlebar-title-xl']/span/text()").get()
        item["original_title"] = response.xpath("//span[contains(text(), 'Titre original')]/following-sibling::strong/text()").get()
        item["global_press_rating"] = response.xpath("//div[@class='rating-item-content']/span[text()=' Presse ']/following-sibling::div//span/text()").get()
        item["global_audience_rating"] = response.xpath("//div[@class='rating-item-content']/span[text()=' Spectateurs ']/following-sibling::div//span/text()").get()
        item["gender"] = response.xpath("//div[@class='meta-body-item meta-body-info']/span/text()").getall()
        item["start_year"] = response.xpath("(//div[@class='meta-body-item meta-body-info']/text())[1]").get()
        item["end_year"] = response.xpath("(//div[@class='meta-body-item meta-body-info']/text())[1]").get()
        item["duration"] = response.xpath("(//div[@class='meta-body-item meta-body-info']/text())[2]").get()
        item["serie_description"] = response.xpath("//p[@class='bo-p']/text()").get()
        item["creator"] = response.xpath("//span[contains(text(), 'Créée par')]/following-sibling::a/text()").getall()
        item["country"] = response.xpath("//span[contains(text(), 'Nationalité')]/following-sibling::span/text()").getall()
        item["seasons"] = response.xpath("//div[@class='stats-numbers-row-item'][1]/div/text()").get()
        item["episodes"] = response.xpath("//div[@class='stats-numbers-row-item'][2]/div/text()").get()
        item["main_actors"] = response.xpath("//span[text()='Avec']/following-sibling::span/text()").getall()
        item["url"] = response.url
        season_episode_page = item["url"].replace("_gen_cserie=", "-")
        season_episode_page = season_episode_page.replace(".html", "/saisons/")
        yield scrapy.Request(season_episode_page, meta={"item":item}, callback=self.parse_season_episode)

    def parse_season_episode(self, response):
        item = response.meta["item"]
        seasons = response.xpath("//h2/a")

        for season in seasons:
            season_url = season.xpath("./@ref").get()
            yield response.follow(season_url, callback=self.parse_season)

    def parse_season(self, response):
        item = response.meta["item"]
        item["season_synopsis"] = response.xpath("//div[@class='txt']/text()").get()
        item["season_audience_rating"] = response.xpath("//span[@class='stareval-note']/text()").get()
        item["episode_title"] = response.xpath("//div[@class='entity-card episode-card entity-card-list cf hred']//div[@class='meta-title']/span/text()").getall()
        item["episode_resume"] = response.xpath("//div[@class='entity-card episode-card entity-card-list cf hred']//div[@class='content-txt synopsis']/text()").getall()    
        item["channel"] = response.xpath("//strong/text()").get()
        casting_url = response.xpath("//h2/a[contains(text(), 'Casting de la saison')]/@href").get()
        yield scrapy.Request(casting_url, meta={"item":item}, callback=self.parse_actor)

    def parse_actor(self, response):
        item = response.meta["item"]
        item["all_actors"] = response.xpath("//section[@class='section casting-actor']//div[@class='md-table-row ']/span[@title]/text() | //section[@class='section casting-actor']//div[@class='md-table-row ']/a[@title]/text() |//div[@class='card person-card person-card-col']/div//a/text() | //div[@class='card person-card person-card-col']/div//span/text()").getall()
        item["directors"] = response.xpath("//h2[contains(text(), 'Réal')]/parent::div/following-sibling::div/a/text() | //h2[contains(text(), 'Réal')]/parent::div/following-sibling::div/span[contains(@title, 'Réal')]/text()").getall()
        item["writers"] = response.xpath("//h2[contains(text(), 'Scénar')]/parent::div/following-sibling::div/a/text() | //h2[contains(text(), 'Réal')]/parent::div/following-sibling::div/span[contains(@title, 'Scénar')]/text()").getall()
        yield item



   



    

