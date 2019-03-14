class DatasetCategory:
    """ Class representing a dataset category.

    Contains regular expressions and function for finding datasets.
    reg - Standard regular expression.
    reg_spaces - Same as self.reg, but with spaces instead of
    underscores. This is required because pdfminer sometimes reads
    underscores as spaces, especially in a document with small font
    size.
    reg_dashes - Same as self.reg, but with dashes instead of
    underscores. Does not works, probably because "-" is a special
    character, and should be "sole dash" or "backslash-dash"
    in different places of regular expressions.
    """

    def __init__(self, name, string):
        self.name = name
        self.reg = re.compile(string, re.X)
        self.reg_spaces = re.compile(string.replace("_", r"\ ")
                                     .replace(r"\w", "a-zA-Z0-9 "), re.X)

    def find(self, text, intervals, datasets):
        strings = []
        (results, text) = find_cut_reg(self.reg, text)
        strings += results
        (results, text) = find_cut_reg(self.reg_spaces, text)
        strings += results
        if strings:
            datasets[self.name] = []
        for s in strings:
            s = s.strip()
            if "INTERVAL" in s:
                if cfg["OPEN_INTERVALS_TEXT"]:
                    nums = re.findall(r"INTERVAL(\d+)!", s)
                    arr = [len(intervals[int(n)]) for n in nums]
                    size = min(arr)
                    # TO DO: If some intervals are shorter then it
                    # should be raised as a warning somewhere...
                    for i in range(0, size):
                        ns = s
                        for n in nums:
                            ns = re.sub("INTERVAL" + n + "!",
                                        intervals[int(n)][i], ns)
                        if self.reg.match(ns):
                            datasets[self.name].append([ns, False])
                        elif self.reg_spaces.match(ns):
                            datasets[self.name].append([ns.replace(" ", "_"),
                                                        "spaces"])
                else:
                    res = 0
                    if self.reg.match(s):
                        res = 1
                    elif self.reg_spaces.match(s):
                        res = 2
                    for i in range(0, len(intervals)):
                        s = re.sub("INTERVAL%d!" % i, intervals[i], s)
                    if res == 1:
                        datasets[self.name].append([s, False])
                    elif res == 2:
                        datasets[self.name].append([s.replace(" ", "_"),
                                                    "spaces"])
            else:
                if self.reg.match(s):
                    datasets[self.name].append([s, False])
                elif self.reg_spaces.match(s):
                    datasets[self.name].append([s.replace(" ", "_"),
                                                "spaces"])
        return (text, datasets)