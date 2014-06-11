#!/usr/bin/env python    
# -*- coding: utf-8 -*-    
from xml.dom import minidom  
#try:
#    import xml.etree.cElementTree as ET
#except ImportError:
import xml.etree.ElementTree as ET
    
    
class XmlParser():
    def __init__(self):  
        self.tree = ET.TreeBuilder
        self.root = ET.Element("root")
          
    def parseXml(self, xmlFile):
        try:
            self.tree = ET.parse(xmlFile)
            self.root = self.tree.getroot()
    #    print "parse xml ok:" + self.root.tag
        except Exception, e:  
            print "Error: cannot parse file:" + xmlFile  

    def get_node(self, tag_name):
        return self.root.find(tag_name)

    def get_node_value(self, tag_name):
	target_node = self.get_node(tag_name)
	if target_node is None :
		return ''
        return target_node.text;

    def get_node_prop(self, tag_name, prop_name):
	target_node = self.get_node(tag_name)
	return self.get_node_propvalue(target_node, prop_name)

	#get the specified prop_key's value for a node
    def get_node_propvalue(self, node, prop_name):
	if node is None :
		return ''

	if prop_name in node.attrib:
        	return node.attrib[prop_name]
	
	return ''

    def get_root_node(self):
        return self.root

    def get_child_node(self, node, tag_name):
	if node is None : 
		return None

        findNode = node.find(tag_name)
        return findNode  

    def get_child_node_value(self, node, tag_name):
	if node is None : 
		return ''

        findNode = node.find(tag_name)
	if findNode is None :
		return ''

        return findNode.text
            
    def get_nodes(self, tag_name):
        return self.root.findall(tag_name)
    
    def get_node_by_index(self, tag_name, idx):
        nodes = self.get_nodes(tag_name);
	if idx > len(nodes)-1 :
		return None

        return nodes[idx]

    def get_node_value_by_index(self, tag_name, idx):
        findNode = self.get_node_by_index(tag_name, idx)
	if findNode is None :
		return ''

        return findNode.text
        
    def is_match(self, node, kv_map):  
        '''''判断某个节点是否包含所有传入参数属性 
        node: 节点 , kv_map: 属性及属性值组成的map'''  
        for key in kv_map:  
            if node.get(key) != kv_map.get(key):  
                return False  
        return True  
        
    def get_nodes_by_keyvalue(self, nodelist, propkv_map):  
        '''''根据属性及属性值定位符合的节点，返回节点  
        nodelist: 节点列表  kv_map: 匹配属性及属性值map'''  
        result_nodes = []  
        for node in nodelist:  
            if self.is_match(node, propkv_map):  
                result_nodes.append(node)  
        
        return result_nodes  

    def get_node_by_keychild(self, nodelist, childMainkv_map):  
        '''''根据child key node定位符合的节点，返回节点'''  
        keyslist = childMainkv_map.keys();
	if len(keyslist) < 1 : 
	    return None

        tmpKey = keyslist[0]
        value =  childMainkv_map[tmpKey];
        for node in nodelist:  
       	    tmp_node = self.get_child_node(node, tmpKey)
       	    tmp_node_value = self.get_child_node_value(node, tmpKey)
	    #print '--test4, key=' + tmpKey + ',node_value=' + tmp_node_value + ',kvalue=' + value
	    if tmp_node is not None : 
		if value == tmp_node_value : 
    			#print 'test5 found'
			return node

        return None 

    def get_custom_node_value(self, parent_tag, childMainkv_map, child_tag):  
	parent_nodes = self.get_nodes(parent_tag)
	if parent_nodes is None : 
		return ''

	ret_value = ''
	curr_node = self.get_node_by_keychild(parent_nodes, childMainkv_map)
	if curr_node <> '' :
		ret_value = self.get_child_node_value(curr_node, child_tag)

	return ret_value

    def print_node(self, node):  
        '''''print a node data'''  
	if node is None : 
		return

        print "********************************************"  
        print "node.attrib:%s" % node.attrib  
        #if node.attrib.has_key("age") > 0 :  
        #    print "node.attrib['age']:%s" % node.attrib['age']  
        
        print "node.tag=%s, text=%s" % (node.tag, node.text)  
        
    
    def printXmlDoc(self):
        print "---print xml doc info----"
        for child in self.root:
            print child.tag, child.attrib      

if __name__ == '__main__':  
    objXmlparser = XmlParser()
    objXmlparser.parseXml('./test.xml')
    objXmlparser.printXmlDoc()
    node1 = objXmlparser.get_node('country')
    objXmlparser.print_node(node1)
    objXmlparser.print_node(objXmlparser.get_node('country/year'))
    print "--rank=" + objXmlparser.get_node_value('country/rank')
    print "--rank2=" + objXmlparser.get_child_node_value(node1, 'rank')
    print "--frist province_name=" + objXmlparser.get_node_value('provinces/province/name')
    print "--second province_name=" + objXmlparser.get_node_value_by_index('provinces/province/name', 1)
    print "--property of name=" + objXmlparser.get_node_propvalue(node1, 'name')

    rootNode = objXmlparser.get_root_node()
    print 'test1=' + objXmlparser.get_child_node_value(rootNode, 'test')
    
    print objXmlparser.get_node_value('test')
    nodes = objXmlparser.get_nodes('country')
    print 'country nodes size=' + str(len(nodes))
    objXmlparser.print_node(nodes[1])

    result_nodes = objXmlparser.get_nodes_by_keyvalue(nodes, {"name":"Singapore2"})
    if len(result_nodes) > 0 :
    	target_node = result_nodes[0]
    	objXmlparser.print_node(target_node)

    result_node = objXmlparser.get_node_by_keychild(nodes, {"id":"3"})
    objXmlparser.print_node(result_node)

    print ' test, province_name:' + objXmlparser.get_custom_node_value('provinces/province', {"id":"12"}, 'name')
