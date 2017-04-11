#!/usr/bin/python3
# Asumes xml a list of items and returns a corresponing dict
import xml.etree.ElementTree as etree
import pprint

def extract_id(elem):
    pass
    try:
        return elem.attrib['id']
    except:
        #Build id from all attributes
        return ';'.join([k +"="+v for k,v in elem.attrib.items()])

def simple_xml2dict(elem):
    item = {}
    for key, value in elem.items():
        result=[]
        
        if isinstance(value, etree.Element):
            result += [';'.join([k +"="+v for k,v in value.attrib.items()])]
            if value.text:
                result += [value.text.strip()]
            children = list(value)
            result += [c.text.strip() for c in children]
        elif value:
            for conscutive_item in value:
                res=[]
                # Attributes
                attributes = conscutive_item.attrib
                if attributes:
                    res.append(conscutive_item.tag+';'.join([k +"="+v for k,v in attributes.items()]))
                
                # Text value
                text = conscutive_item.text
                if text:
                    res.append(text.strip())
                
                #Children Values (No attributes)
                children = list(conscutive_item)
                if children:
                    res.append('{'+';'.join([c.tag+':'+c.text.strip() for c in children])+'}')
                result += res
        item[key] = ' '.join(result).strip() if result else ''
    #pprint.pprint(item)
    return item


def parse(file_path, id_field=None):
    '''
    must return a list of tuples, with each entry:
        (team_name:string, (team_words,):tuple, frequency:int, )
    '''
    print("Accessing source file: {}".format(file_path))
    teams=[]
    tag_stack=[]
    with open(file_path) as xmL:
        current_tag={}
        for event, elem in etree.iterparse(xmL, events=('start', 'end')):
            if event=='start':
                tag_stack.append(elem)

            if event=='end':
                start_elem = tag_stack[-1]
                if len(tag_stack)==2:
                    # We add the parent tag as another value in the record
                    # It needs to be recreated, otherwise all of its children will be readded as its value, duplicating data
                                        #posible id field
                    if id_field:
                        id = etree.Element("_id")
                        id.text=elem.attrib[id_field]
                        print(id.text)
                        current_tag['_id']=id

                    teams.append(simple_xml2dict(current_tag))
                    current_tag={}
                    #break;
                if len(tag_stack)==3:

                    if not current_tag.get(start_elem.tag):

                        #elem.attrib=start_elem.attrib
                        current_tag[start_elem.tag] = elem
                    else:
                        #Convert value to a list of values
                        conscutive_item = current_tag[start_elem.tag]
                        try:
                            current_tag[start_elem.tag]= conscutive_item + [elem]
                        except:
                            current_tag[start_elem.tag]= [elem]
                        
                tag_stack.pop()

    return teams