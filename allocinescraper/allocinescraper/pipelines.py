# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re


class AllocineMoviescraperPipeline:
    def process_item(self, item, spider):
        item = self.clean_press_rating(item)
        item = self.clean_audience_rating(item)
        item = self.clean_year(item)
        item = self.clean_main_actors_list(item)
        item = self.clean_director(item)
        item = self.clean_writer(item)
        item = self.clean_duration(item)
        item = self.clean_gender(item)
        item = self.clean_budget(item)
        item = self.clean_devise(item)
        item = self.clean_boxoffice(item)
        item =self.clean_country(item)
        item = self.clean_language(item)
        return item
    
    def clean_press_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("press_rating")
        if rating is not None:
            adapter["press_rating"] = float(rating.replace(",","."))
        else:
            adapter["press_rating"] = None
        return item
    
    def clean_audience_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("audience_rating")
        if rating is not None:
            adapter["audience_rating"] = float(rating.replace(",","."))
        else:
            adapter["audience_rating"] = None
        return item
    
    def clean_year(self, item):
        adapter = ItemAdapter(item)
        year_str = adapter.get("year")
        if year_str is not None:
            adapter["year"] = int(year_str)
        else:
            adapter["year"] = None
        return item
    
    def clean_main_actors_list(self, item):
        adapter = ItemAdapter(item)
        main_actor_list = adapter.get("main_actors")
        if main_actor_list == []:
            adapter["main_actors"] = []
        else:
            adapter["main_actors"] = main_actor_list[1:]
        return item
    
    def clean_director(self, item):
        adapter = ItemAdapter(item)
        director = adapter.get("director")
        if director == []:
            adapter["director"] = []
        else:
            if "Par" in director:
                index_par = director.index("Par")
                director_list = director[:index_par]
                clean_director_list = director_list[1:]
            else:
                clean_director_list = director[1:]
            adapter["director"] = clean_director_list
        return item
     
    def clean_writer(self, item):
        adapter = ItemAdapter(item)
        writer = adapter.get("writer")
        if writer == []:
            adapter["writer"] = []
        else:
            if "Par" in writer:
                index_par = writer.index("Par")
                writer_list = writer[index_par+1:]
            else:
                writer_list = []
            adapter["writer"] = writer_list
        return item
    
    def clean_duration(self, item):
        adapter = ItemAdapter(item)
        duration = adapter.get("duration")
        if duration == []:
            adapter["duration"] = None
        else:
            duration_str = " ".join(duration)
            duration_str1 = duration_str.strip()
            if "h" in duration_str1:
                hour = int(re.findall(r'\d+', duration_str1)[0])
                if "min" in duration_str1:
                    minutes = int(re.findall(r'\d+', duration_str1)[1])
                else:
                    minutes = 0
            else:
                hour = 0
                minutes = int(re.findall(r'\d+', duration_str1)[0])
            duration_int = 60 * hour + minutes
            adapter["duration"] = duration_int
        return item

    def clean_gender(self, item):
        adapter = ItemAdapter(item)
        gender = adapter.get("gender")
        if gender != []:
            if "|" in gender and "|" in gender[gender.index("|"):]:
                gender = gender[gender.index("|")+2:]
            else:
                gender = gender[gender.index("|")+1:]
            adapter["gender"] = gender
        else:
            adapter["gender"] = None
        return item
    
    def clean_budget(self, item):
        adapter = ItemAdapter(item)
        budget = adapter.get("budget")
        if budget == "-" or budget is None:
            adapter["budget"] = None
        else:
            budget1 = budget.replace(" ", "")
            budget2 = budget1.replace(",","")
            cleaned_budget = re.findall(r'\d+', budget2)[0]          
            adapter["budget"] = int(cleaned_budget)
        return item
    
    def clean_devise(self, item):
        adapter = ItemAdapter(item)
        budget = adapter.get("devise")
        if budget == "-" or budget is None:
            adapter["devise"] = None
        else:
            budget1 = budget.replace(" ", "")
            budget2 = budget1.replace(",","")
            devise = re.findall(r'[a-zA-Z]+', budget2)[0]          
            adapter["devise"] = devise
        return item
    
    def clean_boxoffice(self, item):
        adapter = ItemAdapter(item)
        box_office = adapter.get("box_office")
        if box_office is not None:
            box_office = box_office.replace(" ", "")
            box_office = re.findall(r'\d+', box_office)[0]
            adapter["box_office"] = int(box_office)
        return item
    
    def clean_country(self, item):
        adapter = ItemAdapter(item)
        countries = adapter.get("country")
        for i in range(len(countries)):
            countries[i] = countries[i].strip()
        adapter["country"] = countries
        return item
    
    def clean_language(self, item):
        adapter = ItemAdapter(item)
        language = adapter.get("language")
        for i in range(len(language)):
            language[i] = language[i].strip()
        adapter["language"] = language
        return item

class AllocineSeriescraperPipeline:
    def process_item(self,item,spider):
        item = self.clean_global_press_rating(item)
        item = self.clean_global_audience_rating(item)
        item = self.clean_gender(item)
        item = self.clean_start_year(item)
        item = self.clean_end_year(item)
        item = self.clean_season(item)
        item = self.clean_episode(item)
        item = self.clean_season_audience_rating(item)
        item = self.clean_episode_resume(item)
        return item

    def clean_global_press_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("global_press_rating")
        if rating is not None:
            adapter["global_press_rating"] = float(rating.replace(",","."))
        else:
            adapter["global_press_rating"] = None
        return item
    
    def clean_global_audience_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("global_audience_rating")
        if rating is not None:
            adapter["global_audience_rating"] = float(rating.replace(",","."))
        else:
            adapter["global_audience_rating"] = None
        return item
    
    def clean_gender(self, item):
        adapter = ItemAdapter(item)
        gender = adapter.get("gender")
        if gender != []:
            if "|" in gender and "|" in gender[gender.index("|"):]:
                gender = gender[gender.index("|")+2:]
            else:
                gender = gender[gender.index("|")+1:]
            adapter["gender"] = gender
        else:
            adapter["gender"] = None
        return item
    
    def clean_start_year(self, item):
        adapter = ItemAdapter(item)
        start_year = adapter.get("start_year")
        if start_year is not None:
            start_year = int(re.findall(r'\d+', start_year)[0])
            adapter["start_year"] = start_year
        else:
            adapter["start_year"] = None
        return item

    def clean_end_year(self, item):
        adapter = ItemAdapter(item)
        end_year = adapter.get("end_year")
        if end_year is not None:
            years = re.findall(r'\d+', end_year)
            if len(years) == 2:
                end_year = int(re.findall(r'\d+', end_year)[1])
                adapter["end_year"] = end_year
            elif "Depuis" in end_year:
                adapter["end_year"] = None
            else:
                adapter["end_year"] = end_year
        else:
            adapter["end_year"] = None
        return item

    def clean_duration(self, item):
        adapter = ItemAdapter(item)
        duration = adapter.get("duration")
        if duration is not None:            
            duration_str = duration.strip()
            if "h" in duration_str:
                hour = int(re.findall(r'\d+', duration_str)[0])
                if "min" in duration_str:
                    minutes = int(re.findall(r'\d+', duration_str)[1])
                else:
                    minutes = 0
            else:
                hour = 0
                minutes = int(re.findall(r'\d+', duration_str)[0])
            duration_int = 60 * hour + minutes
            adapter["duration"] = duration_int
        else:
            adapter["duration"] = None
        return item

    def clean_season(self,item):
        adapter = ItemAdapter(item)
        season = adapter.get("season")
        if season is not None:
            season = int(re.findall(r'\d+', season)[0])
        else:
            season = None
        return

    def clean_episode(self,item):
        adapter = ItemAdapter(item)
        episode = adapter.get("episode")
        if season is not None:
            season = int(re.findall(r'\d+', episode)[0])
        else:
            season = None
        return

    def clean_season_audience_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("season_audience_rating")
        if rating is not None:
            adapter["season_audience_rating"] = float(rating.replace(",","."))
        else:
            adapter["season_audience_rating"] = None
        return item
    
    def clean_episode_resume(self, item):
        adapter = ItemAdapter(item)
        episode_resume = adapter.get("episode_resume")
        for i in range(len(episode_resume)):
            episode_resume[i] = episode_resume[i].strip()
        adapter["episode_resume"] = episode_resume
        return item

