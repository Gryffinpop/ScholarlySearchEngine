def process_data(source_csv_file_path):
    import csv
    with open(source_csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [dict(x) for x in reader]
    return data


def do_search(data, query, col, is_number, partial_res):
    import re
    result_list = list()
    if partial_res:
        data = partial_res
    if " and " in query:
        subquery = query.split(' and ', 1)
        query1 = subquery[0]
        query2 = subquery[1]
        result_query1 = do_search(data, query1, col, is_number, partial_res)
        return do_search(data, query2, col, is_number, result_query1)
    elif " or " in query:
        subquery = query.split(' or ', 1)
        query1 = subquery[0]
        query2 = subquery[1]
        or_list = do_search(data, query1, col, is_number, partial_res)
        or_list_2 = do_search(data, query2, col, is_number, partial_res)
        for i in or_list_2:
            if i not in or_list:
                or_list.append(i)
        return or_list
    elif " xor " in query:
        subquery = query.split(' xor ', 1)
        query1 = subquery[0]
        query2 = subquery[1]
        or_list = do_search(data, query1, col, is_number, partial_res)
        or_list_2 = do_search(data, query2, col, is_number, partial_res)
        xor_list = [i for i in (or_list + or_list_2) if (i in or_list and i not in or_list_2) or (i not in or_list and i in or_list_2)]
        return xor_list
    elif query.startswith("not "):
        subquery = query.replace('not ', '', 1)
        if is_number:
            subquery = int(subquery)
            for row in data:
                if subquery != int(row[col]):
                    result_list.append(row)
            return result_list
        elif col == 'authors' or 'categories' or 'keywords':
            for row in data:
                split_list = row[col].split('; ')
                if all(subquery.lower() not in it.lower() for it in split_list):
                    result_list.append(row)
            return result_list
        else:
            for row in data:
                if subquery.lower() not in row[col].lower():
                    result_list.append(row)
            return result_list
    elif "*" in query:
        pattern = (query.lower()).replace('*', '[\w\s\W]*')
        if col == 'authors' or 'categories' or 'keywords':
            for row in data:
                split_list = row[col].split('; ')
                for it in split_list:
                    if re.search(pattern, it.lower()):
                        result_list.append(row)
            return result_list
        else:
            for row in data:
                if re.search(pattern, row[col].lower()):
                    result_list.append(row)
        return result_list
    elif query.startswith("< "):
        if is_number:
            query = int(query.replace('< ', '', 1))
            for row in data:
                if int(row[col]) < query:
                    result_list.append(row)
            return result_list
        elif col == 'authors' or 'categories' or 'keywords':
            query = query.replace('< ', '', 1)
            for row in data:
                split_list = row[col].split('; ')
                if any(it.lower() < query.lower() for it in split_list):
                    result_list.append(row)
            return result_list
        else:
            query = query.replace('< ', '', 1).lower()
            for row in data:
                if row[col].lower() < query:
                    result_list.append(row)
            return result_list
    elif query.startswith("> "):
        if is_number:
            query = int(query.replace('> ', '', 1))
            for row in data:
                if int(row[col]) > query:
                    result_list.append(row)
            return result_list
        elif col == 'authors' or 'categories' or 'keywords':
            query = query.replace('> ', '', 1)
            for row in data:
                split_list = row[col].split('; ')
                if any(it.lower() > query.lower() for it in split_list):
                    result_list.append(row)
            return result_list
        else:
            query = query.replace('> ', '', 1).lower()
            for row in data:
                if row[col].lower() > query:
                    result_list.append(row)
            return result_list
    elif query.startswith("<= "):
        if is_number:
            query = int(query.replace('<= ', '', 1))
            for row in data:
                if int(row[col]) <= query:
                    result_list.append(row)
            return result_list
        elif col == 'authors' or 'categories' or 'keywords':
            query = query.replace('<= ', '', 1)
            for row in data:
                split_list = row[col].split('; ')
                if any(it.lower() <= query.lower() for it in split_list):
                    result_list.append(row)
            return result_list
        else:
            query = query.replace('<= ', '', 1).lower()
            for row in data:
                if row[col].lower() <= query:
                    result_list.append(row)
            return result_list
    elif query.startswith(">= "):
        if is_number:
            query = int(query.replace('>= ', '', 1))
            for row in data:
                if int(row[col]) >= query:
                    result_list.append(row)
            return result_list
        elif col == 'authors' or 'categories' or 'keywords':
            query = query.replace('>= ', '', 1)
            for row in data:
                split_list = row[col].split('; ')
                if any(it.lower() >= query.lower() for it in split_list):
                    result_list.append(row)
            return result_list
        else:
            query = query.replace('>= ', '', 1).lower()
            for row in data:
                if row[col].lower() >= query:
                    result_list.append(row)
            return result_list
    elif is_number:
        query = int(query)
        for row in data:
            if int(row[col]) == query:
                result_list.append(row)
        return result_list
    elif col == 'authors' or 'categories' or 'keywords':
        for row in data:
            split_list = row[col].split('; ')
            if any(query.lower() in it.lower() for it in split_list):
                result_list.append(row)
        return result_list
    else:
        for row in data:
            if query.lower() in row[col].lower():
                result_list.append(row)
        return result_list


def do_pretty_print(data, result_data):
    pretty_list = list()
    if result_data:
        data = result_data
    for row in data:
        pretty_item = pretty_row(row)
        pretty_list.append(pretty_item)
    return pretty_list


def do_top_ten_authors(data):
    from collections import Counter
    authors_list = list()
    for row in data:
        authors_list.extend(row['authors'].split('; '))
    return Counter(authors_list).most_common(10)


def pretty_row(row):
    authors = row['authors'].split('; ')
    for position, name in enumerate(authors):
        splitname = name.split(', ')
        na = splitname[0]
        surname = splitname[1]
        if ' ' in na:
            na = na.split(' ')
            for pos, item in enumerate(na):
                na[pos] = item[0]
            na = ''.join(na)
        else:
            na = na[0]
        final_name = surname + ' ' + na
        authors[position] = final_name
    pretty_authors = ', '.join(authors)
    pretty_item = pretty_authors + '. ' + '(' + row['year'] + ')' + '. ' + row['title'] + '. ' + row['journal'] + ' ' + row['volume'] + '. ' + 'https://doi.org/' + row['doi']
    return pretty_item


def do_publication_tree(data, author):
    from anytree import Node
    root = Node(author)
    dict_years = dict()
    for row in data:
        if author in row['authors']:
            child = row['year']
            if child not in dict_years:
                nodeyear = Node(child, parent=root)
                dict_years[child] = nodeyear
            else:
                nodeyear = dict_years[child]
            pretty_item = pretty_row(row)
            Node(pretty_item, parent=nodeyear)
    return root


def do_coauthor_network(data, author):
    from networkx import DiGraph
    from collections import Counter
    coauthors = list()
    coauthor_network = DiGraph()
    coauthor_network.add_node(author)
    for row in data:
        if author in row['authors']:
            coauthors.extend(row['authors'].split('; '))
            coauthors.remove(author)
    counted_coauthors = Counter(coauthors).items()
    for name, count in counted_coauthors:
        coauthor_network.add_edge(author, name, co_authored_papers=count)
    return coauthor_network


def top_10_volume(data):
    from collections import Counter
    volume_list = []
    for row in data:
        journal_volume_pairs = {row['journal']: row['volume']}
        for key, value in journal_volume_pairs.items():
            volume_list.append((key, value))
    journal_volume_articles = Counter(volume_list).most_common(10)
    journal_volume_articles = [journal_volume_tuple + (article, ) for journal_volume_tuple, article in journal_volume_articles]
    return journal_volume_articles
