# -*- coding: utf-8 -*-
import sys
import string
from selenium import webdriver
from numpy import unique
from time import sleep
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

class MontrealToponymyScraper():
    def __init__(self):
        self.db = MongoDb()
        self._letters = [letter for letter in string.ascii_uppercase] 
        self.urls_to_process = []
        self.address_pairs = []
        self.unique_address = []
        self.errors = []
    
    def run(self, **kwargs):
        option = int(kwargs.get("option", -1))
        if option==-1 or option==1:
            try:
                self._start_driver(**kwargs)
                self.urls_to_process = self._get_url_to_process()
                
                for letter, url in self.urls_to_process.items():
                    print("Working on letter '{0}'".format(letter))
                    try:
                        result = self._get_page(url)
                        if result:
                            self._click_all_results_link()
                            self._parse_address_pairs()
                    except Exception as exc:
                        self.errors.append(str(exc))
                        print("No data for the letter {0}".format(letter))
                self._clean_address()
                self._get_unique_address()
                self._insert_address()
            except Exception as e:
                self.db.log_error(datetime.now(), str(e))
                
    def _get_page(self, url):
        self._driver.get(url)
        return True
        
    def _click_all_results_link(self):
        all_results_element = self._driver.find_element_by_xpath('//*[@id="no_print"]/table/tbody/tr[2]/td/div/a')
        all_results_element.click()        
        return True
        
    def _get_url_to_process(self):
        urls = {}
        for letter in self._letters:
            url = self._get_url_by_letter(letter)
            urls[letter] = url
        return urls

    def _get_url_by_letter(self, letter):
        return "http://ville.montreal.qc.ca/portal/page?_pageid=1560,11245605&_dad=portal&_schema=PORTAL&p_search_type=L&p_search_value={0}".format(str(letter).upper())
    
    def _start_driver(self, **kwargs):
        with_frontend = kwargs.get("display", None)
        if with_frontend:
            self._driver = webdriver.Firefox()
        else:
            self._driver = webdriver.PhantomJS(executable_path="/usr/lib/phantomjs/phantomjs")
        
    def _parse_address_pairs(self):
        table_rows = self._driver.find_elements_by_xpath('//*[@id="print"]/table/tbody/tr/td/table/tbody/tr')[1:]
        n = 0
        for row in table_rows:
            address_pair = row.text.split("\n")
            if len(address_pair)>2:
                address_pair[1]+=address_pair[2]
                address_pair = address_pair[:2]
            self.address_pairs.append(address_pair)
            n +=1
            
        print("fetched {0} address".format(n))
        
    def _clean_address(self):
        for i in range(len(self.address_pairs)):
            first_split = "".join(self.address_pairs[i][0].split(" ")[-1:])
            second_split = first_split.split("'")
            if len(second_split)>1:
                self.address_pairs[i][0] = "".join(second_split[-1:])
            else:
                self.address_pairs[i][0] = "".join(second_split)
                
    def _get_unique_address(self):
        address = []
        address += [x[0] for x in self.address_pairs]
        self.unique_address = unique(address)
        
    def _insert_address(self):
        insert_list = []
        for addr in self.unique_address:
            insert_element = {"name":addr,
                              "letter":addr[0].upper(),
                              "processed":0}
            insert_list.append(insert_element)
            
        self.db.insert_unique_streets(insert_list)
    

class LandRegisterScraper():
    def __init__(self):
        self.db = MongoDb()
        self._landing_url = "http://evalweb.ville.montreal.qc.ca/"
        self._search_url = "http://evalweb.ville.montreal.qc.ca/Role2014actu/RechAdresse.ASP?IdAdrr="
        self._profile_base_url = "http://evalweb.ville.montreal.qc.ca/Role2014actu/roleact_arron_min.asp?ue_id="

    def run(self, **kwargs):
        option = int(kwargs.get("option", -1))
        self._start_driver(**kwargs)
        if option == -1 or option == 2:
            self._get_street_codes()
        if option == -1 or option == 3:
            self._get_profiles_ids()
        if option == -1 or option == 4:
            self._get_profiles()

    def _get_page(self, url):
        self._driver.get(url)
        if u"Veuillez réessayer..." in self._driver.page_source:
            self._driver.get(self._landing_url)
            sleep(1)
            self._driver.get(url)

    def _start_driver(self, **kwargs):
        with_frontend = kwargs.get("display", None)
        if with_frontend:
            self._driver = webdriver.Firefox()
        else:
            self._driver = webdriver.PhantomJS(executable_path="/usr/lib/phantomjs/phantomjs")
        
    def _get_street_codes(self):
        street_names = self.db.unique_streets_collection.find({"processed":0})
        nbr_streets = street_names.count()
        i = 0
        j = 1
        ten_percent = nbr_streets/10
        print("Retrieving street codes for {0} unique street names".format(nbr_streets))
        self._get_page(self._landing_url)
        for street in street_names:
            try:
                form_input_element = self._driver.find_element_by_id("text1")
                form_input_element.send_keys(street["name"])
                form_button_element = self._driver.find_element_by_xpath('//*[@id="div_barreOnglets_eva_a_eva_adresse"]/input[2]')
                form_button_element.click()
                
                results_elements = self._driver.find_elements_by_xpath('//*[@id="select1"]/option')
                for result in results_elements:
                    text = result.text
                    value =  result.get_attribute("value")
                    street_name, borough_name, city_name = (x for x in text.split("/"))
                    street_code, borough_code, city_code = (x for x in value.split("/"))
                    
                    street_dict = {"name":street_name,
                                   "borough":borough_name,
                                   "code":value,
                                   "profiles_ids":[],
                                   "profiles_ids_processed":0,
                                   "profiles_processed":0}
                    self.db.insert_complete_street(street_dict)
                    
                self.db.update_unique_street_status(street["_id"], 1)
                i+=1
                if i>ten_percent*j:
                    print("...............{0}%".format(j*10))
                    j+=1
                    
            except Exception as e:
                self.db.log_error(datetime.now(), str(e)+"##### STREET PROCESSED: {0}".format(street["name"]))
                print("Error researching street : "+street["name"])
                self._get_page(self._landing_url)
                sleep(1)
                pass
            
    def _get_profiles_ids(self):
        complete_streets = self.db.complete_streets_collection.find({"profiles_ids_processed":0}, timeout=False) 
        nbr_streets = complete_streets.count()
        ten_percent_street_ids = nbr_streets /10
        i = 0
        j = 1
        
        print("Retrieving profile ids for {0} streets".format(nbr_streets))
        
        for street in complete_streets:
            try:
                street_profiles_ids = []
                search_url = self._get_search_url_by_id(street["code"])
                self._get_page(search_url)
                results_elements = self._driver.find_elements_by_xpath('//*[@id="select2"]/option')
                
                for result in results_elements:
                    profile_id = result.get_attribute("value")
                    street_profiles_ids.append(profile_id)
                    
                self.db.insert_profiles_ids(street["_id"], street_profiles_ids)
                               
                    
                i+=1
                if i>ten_percent_street_ids*j:
                    print("...............{0}%".format(j*10))
                    j+=1
            except Exception as e:
                self.db.log_error(datetime.now(), str(e)+"##### STREET PROCESSED: {0} ##### BOROUGH: {1}".format(street["name"], street["borough"]))
                    
    def _get_profiles(self):
        print("Parsing and inserting profiles into mongo \nThis will take a while")
        i = 0
        j = 1
        complete_streets = self.db.complete_streets_collection.find({"profiles_ids_processed":1,
                                                                     "profiles_processed":0}, timeout=False)
        nbr_streets = self.db.complete_streets_collection.find({"profiles_ids_processed":1,
                                                                     "profiles_processed":0}).count()
        ten_percent_street_ids = nbr_streets/10
        
        for street in complete_streets:
            try:
                street_profiles = []
                self.db.update_complete_street_status(street["_id"], -1)
                for profile_id in street["profiles_ids"]:
                    parsed_profile = self._parse_profile(profile_id)
                    parsed_profile["quartier"] = street["borough"]
                    parsed_profile["rue"] = street["name"]
                    street_profiles.append(parsed_profile)
                self.db.insert_profiles(street_profiles)
                self.db.update_complete_street_status(street["_id"], 1)
                self.db.log_last_insert(street["name"], 0)
                
                i+=1
                if i>ten_percent_street_ids*j:
                    print("...............{0}%".format(j*10))
                    j+=1
            except Exception as e:
                self.db.log_error(datetime.now(), str(e)+u"##### STREET PROCESSED: {0}".format(street["name"]))
                self.db.log_last_insert(street["name"], 1)
            
        
    def _parse_profile(self, profile_id):
        profile_url = self._get_profile_url_by_id(profile_id)
        self._get_page(profile_url)
        
        row_elements = self._driver.find_elements_by_tag_name("tr")
        i = 0
        j = 8
        for element in row_elements:
            i += 1
            if  u"Caractéristiques de l'unité d'évaluation" in element.text:
                start_3 = i
            elif u"Propriétaire" in element.text:
                start_2 = i
        
        profile = {}
        profile[u"municipalité"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[3]/td[2]').text

            
        profile[u"exercices_financiers"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[4]/td[2]').text
        profile[u"unitée_évaluée"] ={}
        profile[u"unitée_évaluée"]["adresse"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format(j)).text
        if "MONTREAL" in profile[u"municipalité"]:
            profile[u"unitée_évaluée"]["arrondissement"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format(j+1)).text
            j+=1
        profile[u"unitée_évaluée"]["numero_lot"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format(j+1)).text
        profile[u"unitée_évaluée"]["numero_matricule"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format(j+2)).text
        profile[u"unitée_évaluée"]["utilisation_predominante"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format(j+3)).text
        profile[u"unitée_évaluée"]["numero_unité_voisinage"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format(j+4)).text
        profile[u"unitée_évaluée"]["numero_dossier"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format(j+5)).text
        profile[u"propriétaire"] = {}
        profile[u"propriétaire"]["nom"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_2+2))).text
        profile[u"propriétaire"]["statut"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_2+3))).text
        profile[u"propriétaire"]["adresse"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_2+4))).text
        profile[u"propriétaire"]["date_inscription_role"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_2+5))).text
        profile[u"caractéristiques_unité"] = {}
        profile[u"caractéristiques_unité"]["terrain"] = {}
        profile[u"caractéristiques_unité"]["terrain"]["mesure_frontale"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_3+3))).text
        profile[u"caractéristiques_unité"]["terrain"]["superficie"] =  self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_3+4))).text
        profile[u"caractéristiques_unité"]["batiment"] = {}
        profile[u"caractéristiques_unité"]["batiment"]["étages"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+3))).text
        profile[u"caractéristiques_unité"]["batiment"]["année_construction"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+4))).text
        profile[u"caractéristiques_unité"]["batiment"]["aire_étages"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+5))).text
        profile[u"caractéristiques_unité"]["batiment"]["genre_construction"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+6))).text
        profile[u"caractéristiques_unité"]["batiment"]["lien_physique"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+7))).text
        profile[u"caractéristiques_unité"]["batiment"]["nombre_logements"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+8))).text
        profile[u"caractéristiques_unité"]["batiment"]["nombre_locaux_non_residentiels"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+9))).text
        profile[u"caractéristiques_unité"]["batiment"]["nombre_chambres_locatives"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+10))).text
        profile["valeurs"] = {}
        profile["valeurs"]["courant"] = {}
        profile["valeurs"]["courant"][u"date_référence"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_3+15))).text
        profile["valeurs"]["courant"]["valeur_terrain"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_3+16))).text
        profile["valeurs"]["courant"]["valeur_batiment"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_3+17))).text
        profile["valeurs"]["courant"]["valeur_immeuble"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_3+18))).text
        profile["valeurs"][u"antérieur"] = {}        
        profile["valeurs"][u"antérieur"][u"date_référence"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+15))).text
        profile["valeurs"][u"antérieur"]["valeur_role_antérieur"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+16))).text
        profile[u"répartition_fiscale"] = {}        
        profile[u"répartition_fiscale"]["catégorie"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[1]'.format((start_3+22))).text
        profile[u"répartition_fiscale"]["valeur_imposable"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[2]'.format((start_3+23))).text
        profile[u"répartition_fiscale"]["valeur_non_imposable"] = self._driver.find_element_by_xpath('//*[@id="AutoNumber1"]/tbody/tr[{0}]/td[5]'.format((start_3+23))).text
        return profile

    def _get_search_url_by_id(self, street_id):
        return self._search_url + street_id        
        
    def _get_profile_url_by_id(self, profile_id):
        return self._profile_base_url + profile_id
        
      
class MongoDb(MongoClient):
    def __init__(self):
        MongoClient.__init__(self)
        self.db = self.land_register
        self.unique_streets_collection = self.db.unique_streets
        self.complete_streets_collection = self.db.complete_streets
        self.profiles_collection = self.db.profiles
        self.error_log_collection = self.db.error_log
        self.last_insert_log = self.db.last_insert_log
        
    def insert_unique_streets(self, unique_streets_list):
        self.unique_streets_collection.insert(unique_streets_list)
        
    def insert_complete_street(self, street_dictionary):
        self.complete_streets_collection.insert(street_dictionary)
        
    def insert_profiles_ids(self, street_id, profiles_ids_list):
        self.complete_streets_collection.update({"_id":ObjectId(street_id)},
                                                {"$set":{"profiles_ids":profiles_ids_list,
                                                         "profiles_ids_processed":1}})
                                                         
    def insert_profiles(self, profiles_dicts_list):
        self.profiles_collection.insert(profiles_dicts_list)
        
    def update_complete_street_status(self, street_id, status):
        self.complete_streets_collection.update({"_id":ObjectId(street_id)},
                                                {"$set":{"profiles_processed":status}})
        
        
    def update_unique_street_status(self, _id, status):
        self.unique_streets_collection.update({"_id":ObjectId(_id)},
                                              {"$set":{"processed":status}})
    def log_error(self, time, error):
        error = {"date":time, 
                 "error":error}
        self.error_log_collection.insert(error)         

    def log_last_insert(self, street, error):
        logged = self.last_insert_log.count()
        if logged:
            log = self.last_insert_log.find_one()
            self.last_insert_log.update({"_id": ObjectId(log["_id"])}, {"currentDate": datetime.now(), "street":street, "error":error})         
        else:
            self.last_insert_log.insert({"currentDate":datetime.now(), "street":street, "error":error})
                            
        
    
def main(system_arg):  
    mts = MontrealToponymyScraper()
    mts.run(option=system_arg)
    
    lrs = LandRegisterScraper()
    lrs.run(option=system_arg)
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        system_arg = sys.argv[1]
    else:
        system_arg = -1
        
    main(system_arg) 