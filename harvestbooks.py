#!/usr/bin/python
 
# CSC421 group project
# 1. Harvest the book links from gutenberg.org using the robot interface
# 2. Download the books, unzip the content
# 3. Parse title, author and content from the text file
# 4. Insert the parsed book to sqlite database 

import re, urllib, time, sqlite3, zipfile, StringIO

def initialize_database(connection):
    """If the database does not exist, initialize it"""

    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS "authors" ("id"    INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL  UNIQUE , "name" TEXT NOT NULL )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS "books" ("id" INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL  UNIQUE , "author" INTEGER NOT NULL , "title" TEXT NOT NULL , "content" TEXT NOT NULL , "source" TEXT NOT NULL )""")
    connection.commit()


def get_links(data):
    """ Parse the links from html page to a list """

    links = []
    # should match for example 
    # <a href="http://www.gutenberg.lib.md.us/etext05/rome210.zip">
    match = re.findall(r'<a href="([\w:/.?;&=\[\]]*)">', data)
    for link in match:
        if link[-3:] == "zip":
            links.append(link)
        else:
            link = link.replace("&amp;", "&");
            links.append("http://www.gutenberg.org/robot/" + link)
    return links    


def get_book(address, **kwargs):
    """Get the book, unzip, parse and put to database"""

    connection = kwargs["connection"]
    cursor = connection.cursor()
    cursor.execute("select * from books where source=?", (address,))
    if cursor.fetchone():
        print address + " is already in the database\n"
    else:
        # Not in database -> let's download, parse and add it
        
        # Download
        try:
            remotefile = urllib.urlopen(address, "r")
            zipdata = StringIO.StringIO(remotefile.read())
            archive = zipfile.ZipFile(zipdata, "r")
            # We can(?) assume that there is only one file in the archive
            print "getting book from " + address
            bookdata = archive.read(archive.namelist()[0])
            archive.close()
        except Exception, err:
            print Exception, err
            print "ERROR: failed to download/unzip " + address
            return

        # Parse & add
        try:
            (author, title, content) = get_book_information(bookdata)
            insert_to_database(author, title, content, address, **kwargs)
            print "book added to db\n"
        except Exception, err:
            print Exception, err
            print "ERROR: failed to parse book at " + address
            return
    time.sleep(2) # 2 sec delay between downloads as per gutenberg.org instructions


def get_book_information(data):
    """Extracts the author, title and content of the book."""

    #TODO: handle different ebook files since not all of them
    # follow the same format when it comes to author/title/content

    # title
    match = re.findall(r'Title: ([\w ]+)', data)
    title = match[0]

    # author
    match = re.findall(r'Author: ([\w ]+)', data)
    author = match[0]

    # author
    match = re.findall(r'START OF THE PROJECT.+\*\*\*(.+)\*\*\* END OF THE PROJECT', data, re.MULTILINE|re.DOTALL)
    content = match[0]

    return (author, title, content)


def insert_to_database(author, title, content, address, **kwargs):
    """ Inserts book data to sqlite database """

    connection = kwargs["connection"]
    cursor = connection.cursor()
    
    # Find out if the author is already in database
    cursor.execute("select id from authors where name=?", (author,))
    author_id = cursor.fetchone()

    if author_id:
        print author + " is already in the database"
    else:
        print author + " is not in the database -> add"
        # Not in database -> let's add it
        # I don't know what would be the best way to do this
        # so this approach could be a bit ugly... it works, anyway
        cursor.execute("""insert into authors ("id", "name") values (NULL, ?)""", (author,))
        cursor.execute("select id from authors where name=?", (author,))
        author_id = cursor.fetchone()

    # Add the book content to database
    cursor.execute("""insert into books ("id", "author", "title", "content", "source") values (NULL, ?, ?, ?, ?)""", (author_id[0], title, content, address))

    connection.commit()


def main():  
    """ Main function """

    initial_page = "http://www.gutenberg.org/robot/harvest?filetypes[]=txt&langs[]=en"
    database_name = "books.sqlite"
    db_connection = sqlite3.connect(database_name)
    f = urllib.urlopen(initial_page, "r")
    page = f.read()
    f.close()
    initialize_database(db_connection)
    links = get_links(page)
    while len(links):
        if links[0][-3:] == "zip":
            get_book(links.pop(0), connection=db_connection)
        else:
            print "loading new list from " + links[0]
            f = urllib.urlopen(links.pop(), "r")
            page = f.read()
            f.close()
            links = get_links(page)



if __name__ == '__main__':  
    main()  
