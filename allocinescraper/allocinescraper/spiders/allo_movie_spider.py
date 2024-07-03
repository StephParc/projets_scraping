import scrapy
from allocinescraper.items import AllocineMoviescraperItem
import re


class AlloMovieSpider(scrapy.Spider):
    name = "allo_movie_spider"
    allowed_domains = ["allocine.fr"]
    # start_urls = [f"https://allocine.fr/films/?page={i}" for i in range(7957)]
    start_urls = ["https://allocine.fr/films/?page=1"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'allocinescraper.pipelines.AllocineMoviescraperPipeline': 200,
            # 'allocinescraper.pipelines.MovieDatabasePipeline': 300
        }
    }

    def parse(self, response):
        movies = response.xpath("//a[@class='meta-title-link']")
       
        for movie in movies:
            movie_url = movie.xpath("./@href").get()
            yield response.follow(movie_url, callback=self.parse_movie)

    def parse_movie(self, response):
        item = AllocineMoviescraperItem()
        item["id_movie"] = response.xpath("//script[@type='text/javascript'][5]/text()").re(r'"movie_id":"(\d+)')[0]
        item["title"] = response.xpath("//div[@class='titlebar-title titlebar-title-xl']/text()").get()
        item["original_title"] = response.xpath("//span[text()='Titre original ']/following-sibling::span/text() | //div[@class='titlebar-title titlebar-title-xl']/text()").getall()[-1]    
        item["press_rating"] = response.xpath("//div[@class='rating-item']//span[text()=' Presse ']/following-sibling::div//span[@class='stareval-note']/text()").get()
        item["audience_rating"]= response.xpath("//div[@class='rating-item']//span[text()=' Spectateurs ']/following-sibling::div//span[@class='stareval-note']/text()").get()
        item["gender"] = response.xpath("//div[@class='meta-body-item meta-body-info']/span/text()").getall()
        item["year"] = response.xpath("//span[text()='Année de production']/following-sibling::span/text()").get()
        item["duration"] = response.xpath("//div[@class='meta-body-item meta-body-info']/text()").getall()
        item["description"] = response.xpath("//p[@class='bo-p']/text()").get()
        item["main_actors"] = response.xpath("//div[@class='meta-body-item meta-body-actor']/span/text()").getall()
        # item["director"] = response.xpath("//div[@class='meta-body-item meta-body-direction meta-body-oneline']/span/text()").getall()    
        item["writer"] = response.xpath("//div[@class='meta-body-item meta-body-direction meta-body-oneline']/span/text()").getall()
        item["public"] = response.xpath("(//span[@class='certificate-text']/text() | //div[@class='label kids-label aged-default']/text())[1]").get()
        item["country"] = response.xpath("//span[contains(@class, 'nationality')]/text() | //a[contains(@class, 'nationality')]/text()").getall()
        item["language"] = response.xpath("//span[text()='Langues']/following-sibling::span/text()").getall()
        item["box_office"] = response.xpath("//span[text()='Box Office France']/following-sibling::span/text()").get()
        item["budget"] = response.xpath("//span[text()='Budget']/following-sibling::span/text()").get()
        item["devise"] = response.xpath("//span[text()='Budget']/following-sibling::span/text()").get()
        item["url"] = response.url
        casting_page = item["url"].replace("_gen_cfilm=", "-")
        casting_page = casting_page.replace(".html", "/casting/")
        yield scrapy.Request(casting_page, meta={"item":item}, callback=self.parse_people)
        # yield item
        
    def parse_people(self, response):
        item = response.meta["item"]
        # essai -> résultat "[""id1"",""id2""]"
        if response.xpath("//script[contains(text(), 'actor')][2]").re(r'"director":(.*),"nationality"') != []:
            item["director"] = response.xpath("//script[contains(text(), 'actor')][2]").re(r'"director":(.*),"nationality"')
            item["all_actors"] = response.xpath("//script[contains(text(), 'actor')][2]").re(r'"actor":(.*),"director"')
        else:
            item["director"] = response.xpath("//script[contains(text(), 'actor')][1]").re(r'"director":(.*),"nationality"')
            item["all_actors"] = response.xpath("//script[contains(text(), 'actor')][1]").re(r'"actor_voice":(.*),"actor_local_voice"')
        
        item["role"] = response.xpath("//section[contains(@class,'casting-actor')]//div[@class='md-table-row ']/*[last()]/text() | //*[contains(text(), 'Rôle')]/text()").getall()
        
        # essai boucle directors
        # directors = response.xpath("//script[contains(text(), 'actor')][2]").re(r'"director":(.*),"nationality"')
        # for i in range(len(directors)):
        #     directors[i] = directors[i].replace('"','')
        #     directors[i] = int(directors[i])
        
        #  movies id 9 5 6 4 8
        # for director in item["director"]:
        #     perso_item = AllocineMoviescraperItem(item)
        #     perso_url = f"https://www.allocine.fr/personne/fichepersonne_gen_cpersonne={director}.html"
        #     #yield response.follow(perso_url, meta={"item":perso_item}, callback=self.parse_person)
        #     yield scrapy.Request(perso_url, meta={"item":perso_item}, callback=self.parse_person)

        # essai de boucle directors movies2 id 9 5 6
        # for i in range(len(item["director"])):    
        #     fiche_perso_url = f"https://www.allocine.fr/personne/fichepersonne_gen_cpersonne={item["director"][i]}.html"
        #     # yield scrapy.Request(fiche_perso_url, meta={"item":item}, callback=self.parse_person)
        #     # movies3 id 7 5 6
        #     yield response.follow(fiche_perso_url, meta={"item":item}, callback=self.parse_person)
        
        yield item
    
    def parse_person(self, response):
        item = response.meta["item"]
        item["id_person"] = response.url
        item["name"] = response.xpath("//div[contains(@class, 'titlebar-title')][1]/text()").get()
        item["birth_name"] = response.xpath("//span[text()='Nom de naissance ']/following-sibling::h2/text()").get()
        item["nationality"] = response.xpath("//span[contains(text(), 'Nationalité')]/following-sibling::div/text()").getall()
        item["birth_date"] = response.xpath("//span[contains(text(), 'Naissance')]/following-sibling::span/text()").get()
        item["total_roles"] = response.xpath("//div[text()='films et séries tournés']/preceding-sibling::div/text()").get()
        item["nominations"] = response.xpath("//div[text()='prix']/preceding-sibling::div/text()").get()
        item["biographie"] = response.xpath("//div[@class='content-txt ']/text()").get()
        yield item
