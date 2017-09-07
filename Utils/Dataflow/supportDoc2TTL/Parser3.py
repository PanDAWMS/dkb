import codecs
import hashlib
import json
import uuid

def getSupportDocID(dictSupportDocs, documentid):
    docGUID = 'None'
    if documentid in dictSupportDocs.values():
        for position, i in enumerate(dictSupportDocs.values()):
            if i == documentid:
                docGUID = list(dictSupportDocs.keys())[position]
                return docGUID
    if docGUID == 'None':
        fDocumentID = open("supportingDocumentID.txt", "a")
        print("New supportingdocumentID")
        docGUID = uuid.uuid4()
        fDocumentID.write(str(docGUID) + " " + documentid + "\n")
    fDocumentID.close()
    return docGUID

def getAuthorID(dictAuthors, authorid):
    authorGUID = 'None'
    if authorid in dictAuthors.values():
        for position, i in enumerate(dictAuthors.values()):
            if i == authorid:
                authorGUID = list(dictAuthors.keys())[position]
                return authorGUID
    if authorGUID == 'None':
        print("New authorID")
        fAuthorID = open("authorsSupportDoc.txt", "a")
        authorGUID = uuid.uuid4()
        fAuthorID.write(str(authorGUID) + " " + str(authorid) + "\n")
    fAuthorID.close()
    return authorGUID

dictAuthorSupportDocs = {}
fAuthorSDID = open("authorsSupportDoc.txt", "r")
for columns in (raw.strip().split() for raw in fAuthorSDID):
    dictAuthorSupportDocs[columns[0]] = columns[1]

dictSupportDocs = {}
fSupportDcs = open("supportingDocumentID.txt", "r")
for columns in (raw.strip().split() for raw in fSupportDcs):
    dictSupportDocs[columns[0]] = columns[1]

with open("cds_int_notes.json") as data_file:
    data = json.load(data_file)
outfile = codecs.open("list_of_supporting_papers.ttl", 'w', "utf-8")

graph = "http://nosql.tpu.ru:8890/DAV/ATLAS"
ontology = "http://nosql.tpu.ru/ontology/ATLAS"

for item in data:
    docObj = "SupportingDocument/%s" % getSupportDocID(dictSupportDocs, item["GLANCE_ID"])
    docSubject = "%s/%s" % (graph, docObj)
    CDSID = ""
    CDSInternal = ""
    PublicationYear = ""
    URL_Fulltext = ""
    Abstract = ""

    if "CDS_ID" in item:
        CDSID = item["CDS_ID"]
    if "Abstract" in item:
        Abstract = item["Abstract"]
    if "CDSInternal" in item:
        CDSInternal = item["CDSInternal"]
    if "publicationYear" in item:
        PublicationYear = item["publicationYear"]
    if "URL_Fulltext" in item:
        URL_Fulltext = item["URL_Fulltext"]
    attrsSupportDoc = {
        'docSubject': docSubject,
        'graph': graph,
        'ontology': ontology,
        'CDS_ID': CDSID,
        'Abstract': Abstract.translate(str.maketrans({"\\": r"\\", "'": r"\'", "\n": r"\\n"})),
        'CDSInternal': CDSInternal,
        'publicationYear': PublicationYear,
        'URL_Fulltext': URL_Fulltext
    }
    tripletsDocument = '''{docSubject} a <{ontology}#SupportingDocument> .\n'''.format(**attrsSupportDoc)
    outfile.write(tripletsDocument)
    if CDSID != '':
        triplethasCDSID = '''{docSubject} <{ontology}#hasCDS_ID> '{CDS_ID}' .\n'''.format(**attrsSupportDoc)
        outfile.write(triplethasCDSID)
    if CDSInternal != '':
        triplethasCDSInternal = '''{docSubject} <{ontology}#hasCDSInternal> '{CDSInternal}' .\n'''.format(**attrsSupportDoc)
        outfile.write(triplethasCDSInternal)
    if PublicationYear != '':
        triplethasPublicationYear = '''{docSubject} <{ontology}#hasPublicationYear> {publicationYear} .\n'''.format(**attrsSupportDoc)
        outfile.write(triplethasPublicationYear)
    if URL_Fulltext != '':
        triplethasURLFulltext = '''{docSubject} <{ontology}#hasURLFulltext> '{URL_Fulltext}' .\n'''.format(**attrsSupportDoc)
        outfile.write(triplethasURLFulltext)
    if Abstract != '':
        triplethasAbstract = '''{docSubject} <{ontology}#hasAbstract> '{Abstract}' .\n'''.format(**attrsSupportDoc)
        outfile.write(triplethasAbstract)

    if "keywords" in item:
        Keywords = item["keywords"]
        for keyword in Keywords:
            attrKeyword = {
                'docSubject': docSubject,
                'graph': graph,
                'ontology': ontology,
                'Keyword': keyword
            }
            tripletKeyword = '''{docSubject} <{ontology}#hasKeyword> '{Keyword}' .\n'''.format(**attrKeyword)
            outfile.write(tripletKeyword)

    if "authors" in item:
        authors = item["authors"]
        email = ""
        full_name = ""
        for author in authors:
            if "email" in author:
                email = author["email"]
                break
            if "full_name" in author:
                full_name = author["full_name"].split(",")
            else:
                print("full_name is empty")
                break
            hexAuthor = hashlib.sha1(full_name.encode('utf-8')).hexdigest()
            authorUID = getAuthorID(dictAuthorSupportDocs, hexAuthor)
            authorName = "Person/%s", authorUID
            authorSubject = "<%s/%s>" % (graph, authorName)
            affiliation = ""
            full_name = ""
            if "affiliation" in author:
                affiliation = author["affiliation"]
            attrrsAuthor = {
                'docSubject': docSubject,
                'authorUID': authorUID,
                'authorSubject': authorSubject,
                'graphURL': graph,
                'affiliation': affiliation,
                'email': email,
                'full_name': full_name
            }
            tripletsAuthor = '''{authorSubject} a <{ontology}#Person> . \n'''.format(**attrrsAuthor)
            outfile.write(tripletsAuthor)
            if full_name != '':
                triplethasFullName = '''{authorSubject} <{ontology}#hasFullName> '{full_name}' .\n'''.format(
                    **attrrsAuthor)
                outfile.write(triplethasFullName)
            if email != '':
                triplethasEmail = '''{authorSubject} <{ontology}#hasEmail> '{email}' .\n'''.format(
                    **attrrsAuthor)
                outfile.write(triplethasEmail)
            if affiliation != '':
                triplethasAffiliation = '''{authorSubject} <{ontology}#hasAffiliation> '{affiliation}' .\n'''.format(
                    **attrrsAuthor)
                outfile.write(triplethasAffiliation)
            hasAuthorTriplet = "{docSubject} <{ontology}#hasAuthor> {authorSubject} .\n".format(**attrrsAuthor)
            outfile.write(hasAuthorTriplet)

outfile.close()
