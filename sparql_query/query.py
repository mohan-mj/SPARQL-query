
import SPARQLWrapper
from SPARQLWrapper import JSON, XML, N3
from rdflib import Graph
import xml.dom.minidom as md
import pandas as pd

# import query_list as ql

class QUERY:
    """[summary]
    """
    def __init__(self, repo="BtDom") -> None:
        """[summary]
        """
        self.sparql = SPARQLWrapper.SPARQLWrapper("http://md2ec0bc:7200/repositories/"+ repo)
        self.result = None
        self.format = JSON
        self.query_string = ""
        print(f'SPARQLWrapper expected \t\t --> 1.8.5')
        print(f'SPARQLWrapper version used \t --> {SPARQLWrapper.__version__}')

    def get_results(self, format=JSON, query_string=None):
        """ query functtion"""
        # if format:
        #     self.format = format
        if query_string:
            self.query_string = query_string
        self.sparql.setQuery(self.query_string)  # set query
        
        def __get():
            try :
                return self.sparql.query().convert()  # query initiate
            except :
                print("Query failed")

        if format == JSON:
            self.sparql.setReturnFormat(format)
            self.result = __get()
            
        elif format == XML:
            self.sparql.setReturnFormat(format)
            dom = md.parseString(__get().toxml())     
            self.result = dom.toprettyxml()
            
        elif format == N3:
            self.sparql.setReturnFormat(format)
            g = Graph()
            g.parse(data=__get(), format="n3")
            return g.serialize(format='n3')
    
    def parser(self, header=["objName", "zone"]):
        results_pd = pd.DataFrame(self.result["results"]["bindings"])
        def __parser(x):
            t = list()
            for h in header:
                t.append(x[h]['value'])
            return t
        results_pd = results_pd.apply(__parser,  result_type='broadcast', axis=1)
        return results_pd
            
if __name__ == "__main__":
    # QUERY class
    query = QUERY("sample_employee")
    
    # query.get_results('JSON', query_string)
    # print(query.result)  
    query_string = """ 
    
    PREFIX vcard:<http://www.w3.org/2006/vcard/ns#> 

    SELECT ?person
    WHERE
    { ?person vcard:family-name "Smith"}
    
    """
    
    query.get_results(JSON, query_string) # ql.query_dict['degC']
    print(query.result) 
    
    query_result_pd = query.parser(header=["person"])
    print(query_result_pd)
