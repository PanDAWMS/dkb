"""
PDF Analyzer script for processing tables
"""

import re

# TO DO: improve this.
PAGE_SIZE = 1000

re_textline = re.compile("<textline bbox=\"[0-9.,]+\">.+?</textline>",
                         re.DOTALL)
re_table_caption_short = re.compile(r"Table (\d+):")


class TextLine:
    """ Class which represents text lines.

    Each line has 4 coords, text and coordinates of spaces.
    params - can be either text or list. In case of text it is processed
    as a line from xml file. In case of list, a new copy of TextLine is
    created from it - this is used when we are splitting or merging
    existing lines.
    """
    re_bbox = re.compile("bbox=\"([0-9.,]+)\"")
    re_text_symbol = re.compile("<text font.+>([^ ]+)</text>")
    r = "<text font=\"(.+)\" bbox=\".+\" size=\"(.+)\">([^ ]+)</text>"
    re_text_symbol_params = re.compile(r)
    re_text_space = re.compile("<text> </text>")

    def __init__(self, params=False, text_symbols=None):
        if isinstance(params, str) or isinstance(params, str):
            lines = params.split("\n")

            self.text = ""
            if text_symbols is not None:
                self.symbols = {}
                for line in lines:
                    f = self.re_text_symbol_params.match(line)
                    if f:
                        self.text += f.group(3)
                        font = f.group(1)
                        size = float(f.group(2))
                        if (font, size) in text_symbols:
                            text_symbols[font, size] += 1
                        else:
                            text_symbols[font, size] = 1
                        if (font, size) in self.symbols:
                            self.symbols[font, size] += 1
                        else:
                            self.symbols[font, size] = 1
                    f = self.re_text_space.match(l)
                    if f:
                        self.text += " "
                return None

            self.spaces_coords = []
            after_space = False
            first = lines.pop(0)
            coords = self.re_bbox.search(first).group(1)
            # Pdf miner has Y-axis pointed upwards, which does not suits
            # us for several reasons.
            [self.left, self.bottom, self.right, self.top] = coords.split(",")
            self.left = float(self.left)
            self.right = float(self.right)
            self.top = float(self.top)
            self.bottom = float(self.bottom)

            for line in lines:
                f = self.re_text_symbol.match(line)
                if f:
                    self.text += f.group(1)
                    coords = self.re_bbox.search(line)
                    if coords:
                        [l0, t0, r0, b0] = coords.group(1).split(",")
                        if after_space:
                            self.spaces_coords[-1][1] = float(l0)
                            after_space = False
                    continue
                f = self.re_text_space.match(line)
                if f:
                    self.text += " "
                    self.spaces_coords.append([float(r0), 0])
                    after_space = True

        elif isinstance(params, list):
            [self.left, self.top, self.right, self.bottom,
             self.text, self.spaces_coords] = params

            self.center = [(self.left + self.right) / 2,
                           (self.top + self.bottom) / 2]

    def swap_y(self, top):
        """ Change Y-coords according to new Y-axis. """
        self.top = top - self.top
        self.bottom = top - self.bottom
        self.center = [(self.left + self.right) / 2,
                       (self.top + self.bottom) / 2]

    def same_row(self, line):
        """ Determine whether line and self belong to the same row.

        Y coordinates are compared to do so.
        """
        if line.center[1] >= self.top and line.center[1] <= self.bottom:
            return True
        else:
            return False

    def split(self, space):
        """ Split self into two lines by breaking over the space. """
        words = self.text.split()
        words1 = words[:self.spaces_coords.index(space) + 1]
        words2 = words[self.spaces_coords.index(space) + 1:]
        left1 = self.left
        right1 = space[0]
        text1 = " ".join(words1)
        spaces1 = self.spaces_coords[:self.spaces_coords.index(space)]
        nl1 = TextLine([left1, self.top, right1, self.bottom, text1, spaces1])
        left2 = space[1]
        right2 = self.right
        text2 = " ".join(words2)
        spaces2 = self.spaces_coords[self.spaces_coords.index(space) + 1:]
        nl2 = TextLine([left2, self.top, right2, self.bottom, text2, spaces2])
        return [nl1, nl2]

    def merge(self, line):
        """ Merge two overlapping lines. """
        left = min(self.left, line.left)
        top = min(self.top, line.top)
        right = max(self.right, line.right)
        bottom = max(self.bottom, line.bottom)
        text = self.text + line.text
        spaces = self.spaces_coords + line.spaces_coords
        nl = TextLine([left, top, right, bottom, text, spaces])
        return nl


def row_centery(row):
    """ Calculate average y-center in a row. """
    return sum([line.center[1] for line in row]) / len(row)


class Table:
    re_month = re.compile("(january|february|march|april|may|june|july|august"
                          "|september|october|november|december)")

    def __init__(self, caption, rows, caption_position):
        # Table description
        self.caption = caption

        # Remove rows which contain date - this is used because
        # sometimes date stamped on a page gets caught while we are
        # searching for table lines.
        self.rows = []
        for row in rows:
            date_row = False
            for line in row:
                if self.re_month.search(line.text.lower()):
                    date_row = True
                    break
            if not date_row:
                row.sort(key=lambda line: line.center[0])
                self.rows.append(row)

        # Separate table lines from text above table. This is done by
        # looking for a space between rows which is too large.
        max_diff = False
        if caption_position < 0:
            r = 0
            while r < len(self.rows) - 1:
                if not max_diff:
                    max_diff = row_centery(self.rows[r + 1])\
                        - row_centery(self.rows[r])
                else:
                    diff = row_centery(self.rows[r + 1]) - \
                        row_centery(self.rows[r])
                    if diff > 1.4 * max_diff:
                        del self.rows[r + 1:]
                        break
                    elif diff > max_diff:
                        max_diff = diff
                r += 1
        else:
            r = len(self.rows) - 1
            while r > 0:
                if not max_diff:
                    max_diff = row_centery(self.rows[r])\
                        - row_centery(self.rows[r - 1])
                else:
                    diff = row_centery(self.rows[r]) - \
                        row_centery(self.rows[r - 1])
                    if diff > 1.4 * max_diff:
                        del self.rows[:r]
                        break
                    elif diff > max_diff:
                        max_diff = diff
                r -= 1

        # Find overlapping (by X coordinate) lines and merge them. So
        # far this was required to deal with rows where several lines
        # are above each other. No additional spaces are added.
        rows = []
        for row in self.rows:
            new_row = []
            line = row[0]
            for i in range(1, len(row)):
                if line.right < row[i].left:
                    new_row.append(line)
                    line = row[i]
                else:
                    line = line.merge(row[i])
            new_row.append(line)
            rows.append(new_row)
        self.rows = rows

        # If not all rows have same length, attempt to break short rows.
        if self.rows:
            i = 0
            while True:
                len_min = len(min(self.rows, key=lambda row: len(row)))
                len_max = len(max(self.rows, key=lambda row: len(row)))
                if len_min == len_max:
                    break
                else:
                    self.break_short_rows(len_max)
                    i += 1
                if i == 2:
                    # Too much breaking attempts.
                    break

    def construct_row(self, first_line, used_lines):
        """ Construct a row which contains given line.

        Lines which were already used are not used again.
        """
        row = [first_line]
        for line in self.lines:
            if line != first_line and line not in used_lines \
                    and first_line.same_row(line):
                row.append(line)
        return row

    def row_text(self, num=None, row=False):
        """ Return a combined text of all row lines.

        Spaces between lines are replaced with "!".
        """
        if num is not None or num == 0:
            row = self.rows[num]
        return "!".join([line.text for line in row])

    def break_short_rows(self, max_elements):
        """ Attempt to break lines in rows which are too short. """
        normal_rows = []
        short_rows = []
        # Divide rows into short and normal.
        for row in self.rows:
            if len(row) == max_elements:
                normal_rows.append(row)
            else:
                short_rows.append(row)
        main_row = normal_rows[0]
        # Calculate x coord for centers of each column in first normal
        # row.
        main_centers = [(line.left + line.right) / 2 for line in main_row]
        # Calculate x boundaries of each column.
        boundaries = []
        for i in range(0, max_elements):
            # Most left point in a column.
            boundaries.append(min(normal_rows,
                                  key=lambda row: row[i].left)[i].left)
            # Most right point in a column.
            boundaries.append(max(normal_rows,
                                  key=lambda row: row[i].right)[i].right)
        del boundaries[0]
        del boundaries[-1]
        # Transform boundaries into spaces between columns.
        column_spaces = []
        for i in range(0, len(boundaries) / 2):
            column_spaces.append([boundaries[2 * i], boundaries[2 * i + 1]])
        self.rows = normal_rows
        # Attempt to break some of the lines in short rows.
        for row in short_rows:
            new_row = []
            for line in row:
                for cs in column_spaces:
                    # If line crosses the space entirely, attempt to
                    # break it on one of the spaces in it.
                    if line.left < cs[0] and line.right > cs[1]:
                        # We cannot break a line if there are no spaces.
                        # This means that line is, most likely, not needed.
                        if not line.spaces_coords:
                            line = None
                            break
                        else:
                            # Find the line space closest to column space.
                            cs2 = (cs[1] + cs[0]) / 2
                            cls = min(line.spaces_coords,
                                      key=lambda space:
                                      abs((space[1] + space[0]) / 2 - cs2))
                            if abs((cls[1] + cls[0]) / 2 - cs2) > 5000:
                                # Line's spaces are too far from the
                                # column boundaries, so it is removed.
                                # TO DO: fix this.
                                line = None
                                break
                            [nl1, nl2] = line.split(cls)
                            new_row.append(nl1)
                            line = nl2
                if line is not None:
                    new_row.append(line)
            # Make sure that new row has enough members.
            if new_row:
                num_lines = len(new_row)
                # Try to find a line corresponding each main center.
                for c in main_centers:
                    if num_lines >= max_elements:
                        # Row is long enough.
                        break
                    i = 0
                    existing_column = False
                    for line in new_row:
                        if line.left <= c and line.right >= c:
                            existing_column = True
                            break
                    # If there is no line corresponding a center, add
                    # an "EMPTY" line to row.
                    if not existing_column:
                        nl = TextLine([c - 1, new_row[0].top, c + 1,
                                       new_row[0].bottom, "EMPTY", []])
                        new_row.append(nl)
                        num_lines += 1
                new_row.sort(key=lambda line: line.center[0])
                self.rows.append(new_row)
        self.rows.sort(key=lambda row: row_centery(row))


def analyze_page(text):
    lines = [TextLine(line) for line in re_textline.findall(text)]

    # Find the highest top coordinate possible and use it as a zero
    # point for new Y axis.
    top = max(lines, key=lambda line: line.top).top
    for line in lines:
        line.swap_y(top)

    t = []
    rows = []
    # Construct rows out of lines
    for line in lines:
        if line not in t:
            row = [line]
            for nline in lines:
                if nline != line and nline not in t and line.same_row(nline):
                    row.append(nline)
            row.sort(key=lambda nline: nline.center[0])
            rows.append(row)
            t += row
    rows.sort(key=lambda row: row_centery(row))
    # Discard the top row if it contains "DRAFT" line. Such line is sometimes
    # placed on top of each page of the document and may interfere with
    # reconstruction when the caption is below the tables.
    for line in rows[0]:
        if line.text == "DRAFT":
            rows = rows[1:]
            break

    return rows


def get_tables_from_text(text):
    """ Get tables from a xml page text. """
    rows = analyze_page(text)
    caption_start = False
    caption_rows = []
    text_rows = []
    # Determine the type of each row, if possible.
    # Caption rows are related to the table - notably, they contain the table
    # number.
    # Text rows contain parts of the document's main text body.
    # The tables cannot include rows from any of these two types - therefore,
    # such rows (and document borders) can be used to determine whether the
    # table captions are positioned above or below their tables.
    for (i, row) in enumerate(rows):
        if re_table_caption_short.match(row[0].text):
            caption_start = row
            caption_rows.append(i)
        elif caption_start and len(row) == 1 and\
                abs(row[0].left - caption_start[0].left) < 1.0:
            caption_rows.append(i)
        else:
            caption_start = False
            if len(row) > 1 and row[0].right - row[0].left < 10.0:
                f = row[1]
            else:
                f = row[0]
            # Magic number - text rows in most of the documents are positioned
            # in such way that PDFMiner determines their left coordinate to
            # be ~76. Non-rotated pages only.
            if f.left - 76.0 < 1.0 and len(row) < 3:
                text_rows.append(i)
    # Tables' captions position on the page
    # -1 - above tables
    # 0 - unknown
    # 1 - below tables
    caption_position = 0
    if 0 in caption_rows:
        caption_position = -1
    elif len(rows) - 1 in caption_rows:
        caption_position = 1
    else:
        for i in caption_rows:
            if i - 1 in text_rows:
                caption_position = -1
                break
            elif i + 1 in text_rows:
                caption_position = 1
                break
    tables = []
    if caption_position < 0:
        i = len(rows) - 1
        last_unused = i
        while i >= 0:
            if i in caption_rows:
                table_rows = rows[i + 1:last_unused + 1]
                while i - 1 in caption_rows:
                    i -= 1
                caption = "".join([line.text for line in rows[i]])
                while i + 1 in caption_rows:
                    i += 1
                    caption += "".join([line.text for line in rows[i]])
                while i - 1 in caption_rows:
                    i -= 1
                # Row after table cannot be another caption and is first unused
                # row for the next table.
                i -= 1
                last_unused = i
                i -= 1
                table = Table(caption, table_rows, caption_position)
                if table.rows:
                    tables.append(table)
            else:
                i -= 1
        tables = list(reversed(tables))
    else:
        i = 0
        last_unused = i
        while i < len(rows):
            if i in caption_rows:
                table_rows = rows[last_unused:i]
                caption = "".join([line.text for line in rows[i]])
                while i + 1 in caption_rows:
                    i += 1
                    caption += "".join([line.text for line in rows[i]])
                # Row after table cannot be another caption and is first unused
                # row for the next table.
                i += 1
                last_unused = i
                i += 1
                table = Table(caption, table_rows, caption_position)
                if table.rows:
                    tables.append(table)
            else:
                i += 1
    return tables
