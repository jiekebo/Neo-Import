#!/usr/local/bin/python

import csv
import requests
from py2neo import neo4j, node, rel
from progressbar import ProgressBar, Percentage, Bar

def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    print "Importing contact groups:"
    contactGroups = importGroups(graph_db)
    print "Importing contacts:"
    importContacts(graph_db, contactGroups)


def importGroups(graph_db):
    contactGroups = dict()
    count = 0
    for line in open('recipientGroups.csv').xreadlines(  ): count += 1
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=count).start()    

    groupsIndex = graph_db.get_or_create_index(neo4j.Node, "Groups")
    with open('recipientGroups.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            pbar.update(reader.line_num)
            contactGroup = graph_db.get_or_create_indexed_node("Groups", "id", row[0], {"id":row[0], "name":row[1], "user_id":row[2]})
            contactGroup.add_labels("group")
            contactGroups[row[0]] = contactGroup
    pbar.finish()
    return contactGroups


def importContacts(graph_db, contactGroups):
    contactsIndex = graph_db.get_or_create_index(neo4j.Node, "Contacts")
    count = 0
    for line in open('recipientList.csv').xreadlines(  ): count += 1
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=count).start()

    with open('recipientList.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            pbar.update(reader.line_num)
            contact = graph_db.get_or_create_indexed_node("Contacts", "msisdn", row[1], {"msisdn":row[1], "firstname":row[2], "lastname":row[3], "user_id":row[5]})
            contact.add_labels("contact")
            try:
                contactGroup = contactGroups[row[4]]
                graph_db.create(
                    rel(contactGroup, "OWNS", contact)
                )
            except Exception, e:
                pass
    pbar.finish()


if __name__ == '__main__':
    main()