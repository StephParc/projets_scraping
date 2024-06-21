import scrapy
from moviescraper.items import SeriescraperItem

class SeriespiderSpider(scrapy.Spider):
    name = "seriespider"
    allowed_domains = ["imdb.com"]
    start_urls = ["https://www.imdb.com/chart/toptv/?ref_=nv_tvv_250"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'moviescraper.pipelines.SeriescraperPipeline': 400
        }
    }

    def parse(self, response):
        series = response.xpath("//a[@class='ipc-title-link-wrapper']")
       
        for serie in series:
            serie_url = serie.xpath("./@href").get()
            yield response.follow(serie_url, callback=self.parse_serie)

    def parse_serie(self, response):
        item = SeriescraperItem()
        item['title'] = response.xpath("//h1/span/text()").get()
        item['original_title'] = response.xpath("//h1/following-sibling::div/text()").get()
        item['rating'] = response.xpath("//span[text()='10']/preceding-sibling::span/text()").get()
        item['gender'] = response.xpath("//a[@class='ipc-chip ipc-chip--on-baseAlt']/span/text()").getall()
        item['years'] = response.xpath("//h1/following-sibling::ul/li[2]/a/text()").get()
        item['duration'] = response.xpath("//h1/following-sibling::ul/li[4]/text()").get()
        item['description'] = response.xpath("//span[@class='sc-cafe919b-2 jvoyXJ']/text()").get()
        item['actors'] = response.xpath("(//a[text()='Casting principal'])[1]/following-sibling::div//a/text()").getall()
        item['director'] = response.xpath("//a[@class='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link']/text()").getall()[0]
        item['public'] = response.xpath("//h1/following-sibling::ul/li[3]/a/text()").get()
        item['country'] = response.xpath("//span[contains(text(), 'Pays')]/following-sibling::div//a/text()").get()
        item['language'] = response.xpath("//span[text()='Langues']/following-sibling::div//li/a/text()").getall()
        item['seasons'] = response.xpath("//select[@id='browse-episodes-season']/@aria-label").get()
        item['episode'] = response.xpath("//span[contains(text(), 'Épisodes')]/following-sibling::span/text()").get()
        yield item