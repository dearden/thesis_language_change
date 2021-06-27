import os
import sys
import re
import json
from lxml import etree


# Process a member.
def process_member(member):
    # If there is a member...
    if member is None:
        return None
    # ...Get the member name and id.
    elif "ContinuationText" in member.attrib:
        member_name = member.attrib['ContinuationText']
        member_id = member.attrib['PimsId']
        if "MnisId" in member.attrib:
            mnis = member.attrib['MnisId']
        else:
            mnis = None

        if "xid" in member.attrib:
            xid = member.attrib['xid']
        else:
            mnis = None

        out_dict = {"member_name": member_name,
                    "member_id": member_id,
                    "member_mnis": mnis,
                    "member_xid": xid}
    else:
        out_dict = None

    return out_dict


def get_all_text(curr_element):
    text = curr_element.text
    if text is not None:
        text = text.replace("\n", " ").strip()
    else:
        text = ""

    # If the element has children. Check their tails for text.
    if len(curr_element):
        chillens = list(curr_element)
        for child in chillens:
            if child.tail is not None:
                child_tag = child.tag
                new_text = child.tail
                new_text = new_text.replace("\n", " ").strip()
                text = "{0} {1}".format(text, new_text)

            # if re.fullmatch(r"\{.*\}I", str(child.tag)):
            #     print("Found another fucker")

    return text


# Process a question element.
def process_question(question):
    para = question.find("{*}hs_Para")

    # Get the ID and the member.
    uid = para.attrib['UID']
    member = process_member(para.find(".//{*}Member"))

    # ODD_UIDS =  ["18102932000015"]
    # if uid in ODD_UIDS:
    #     print("Found an odd one")

    # Set the default contribution type to None
    contType = None
    if member is not None:
        # Get the contribution type if there is one.
        if "ContributionType" in para.find(".//{*}Member").attrib:
            contType = para.find(".//{*}Member").attrib['ContributionType']

    # Get the question text.
    qText = question.find(".//{*}QuestionText")
    if qText is not None:
        text = get_all_text(qText)
    else:
        text = get_all_text(para)

    # compare_text = etree.tostring(para, method="text", encoding="unicode")
    # compare_text = compare_text.replace("\n", " ").strip()

    # member_text = etree.tostring(para.find(".//{*}Member"), method="text", encoding="unicode")

    # Create a dictionary to return.
    out_dict = {"type":"Question",
                "uid": uid,
                "member":member,
                "contribution_type":contType,
                "text":text}

    return out_dict

# Process a paragraph element.
def process_para(para):
    # Get the ID and member.
    if "UID" in para.attrib:
        uid = para.attrib['UID']
    else:
        uid = None

    # ODD_UIDS = ["18102932000015"]
    # if uid in ODD_UIDS:
    #     print("Found an odd one")

    member = process_member(para.find(".//{*}Member"))

    # Set the default contribution type to None
    contType = None
    if member is not None:
        # Get the contribution type if there is one.
        if "ContributionType" in para.find(".//{*}Member").attrib:
            contType = para.find(".//{*}Member").attrib['ContributionType']

    text = get_all_text(para)
    # if text is None:
    #     print("OH NO")

    # compare_text = etree.tostring(para, method="text", encoding="unicode")
    # compare_text = compare_text.replace("\n", " ").strip()

    # Create dictionary to output.
    out_dict = {"type":"Paragraph",
                "uid": uid,
                "member":member,
                "contribution_type":contType,
                "text":text}

    return out_dict

# Write a para (or question) to a json file.
def write_para(para):
    with open("{}.json".format(para['id']), "w") as out_file:
        json.dump(para, out_file)

# Function for adding paras to a list. Only adds if it's one we want.
def add_para(para, debate):
    if para is not None:
        if para['member'] is not None:
            if para['text'] is not None and len(para['text']) > 1:
                if para['uid'] not in debate:
                    debate[para['uid']] = para

def reset_vars(variables):
    for var in variables:
        var = None


if __name__ == "__main__":
    # Get the directories from the input parameters.
    if len(sys.argv) > 1:
        xmldir = sys.argv[1]
        outdir = sys.argv[2]
    else:
        xmldir = input("Enter xml directory:\n") # commons-tidy
        outdir = input("Enter out directory:\n") # processed_commons

    # Create the output directory if need be.
    if not os.path.isdir(outdir):
        os.makedirs(outdir)

    # Have some lists of info that we'll use for debugging/understanding.
    all_sections = set()

     # iterate through all files and call the find_all_xmls function.
    for subdir, dirs, files in os.walk(xmldir):
        for filename in files:
            # If it's not an xml file, ignore it.
            if not filename.endswith("xml"):
                continue

            # Get the filepath for the current xml file.
            xmlfile = os.path.join(subdir, filename)

            # create element tree object
            tree = etree.parse(xmlfile)

            # get root element
            root = tree.getroot()
            ns_map = root.nsmap

            # Get the tag containing the commons stuff.
            #house = root.find("{*}House[@name='Commons']")
            debates = root.find(".//{*}System[@type='Debate']")

            # At the beginning of the debates, set topic to none.
            curr_topic = None
            curr_speaker = None
            curr_question = None
            curr_para = None
            curr_section = None
            curr_department = None
            curr_section_tag = None
            curr_xml_file = filename.split(".")[0]

            # Initialise the list of paras for the debate.
            hansard_debate = dict()

            frag_num=1
            # Loop through each fragment and extract the debate info.
            for fragment in debates.iterfind("{*}Fragment"):
                # Extract the header and find the date of the sitting.
                header = fragment.find("{*}Header")
                frag_date = header.find("{*}Sitting").attrib['short-date']

                # Now process the body to get further goodies.
                body = fragment.find("{*}Body")
                body_chillens = list(body)
                for child in body_chillens:
                    try:
                        # Get the current tag and remove the namespace.
                        curr_tag = child.tag

                        # Skip this child if the tag is not a string.
                        if not isinstance(curr_tag, str):
                            continue

                        # remove the gubbins from in front of the actual tag
                        curr_tag = re.match(r"\{.*\}(\w+)", curr_tag).group(1)

                        # Skip this child if the tag is None
                        if curr_tag is None:
                            continue

                        # Check which type of element it is and process accordingly.
                        # Check if it is a Section marker
                        if re.fullmatch("hs_2\w+", curr_tag):
                            curr_section = etree.tostring(child, method="text", encoding="unicode")
                            curr_section = curr_section.replace("\n", " ").strip()
                            all_sections.add((curr_section, curr_tag))
                            add_para(curr_para, hansard_debate)
                            # Set everything to be None
                            curr_para = None
                            curr_topic = None
                            curr_speaker = None
                            curr_department = None
                            curr_question = None
                            curr_section_tag = curr_tag

                        # Check if it's an Oral Answers marker (for some reason separate)
                        elif re.fullmatch("hs_3OralAnswers", curr_tag):
                            if child.text is not None:
                                curr_section = child.text.replace("\n", " ").strip()
                                all_sections.add((curr_section, curr_tag))
                                add_para(curr_para, hansard_debate)
                                # Set everything to be None
                                curr_para = None
                                curr_topic = None
                                curr_speaker = None
                                curr_department = None
                                curr_question = None
                                # Update the current section tag
                                curr_section_tag = curr_tag
                            else:
                                pass

                         # Check if it's a Department tag.
                        elif re.fullmatch("hs_6bDepartment", curr_tag):
                            department = child.find(".//{*}DepartmentName")
                            if department is not None:
                                if department.text is not None:
                                    curr_department = department.text.replace("\n", " ").strip()
                                else:
                                    pass
                            else:
                                pass

                        # Check if it is a question topic.
                        elif re.fullmatch("hs_8\w+", curr_tag):
                            if child.text is not None:
                                topic = child.text.replace("\n", " ").strip()
                                curr_topic = topic
                            else:
                                pass

                        # Check if it is a question.
                        elif curr_tag == "Question":
                            # Ignore certain sections.
                            if curr_section_tag == "hs_2BusinessWODebate":
                                continue

                            # Questions contain a normal para element.
                            question = process_question(child)

                            # Set the current speaker in case next paragraph doesn't specify.
                            curr_speaker = question['member']

                            # Set the topic.
                            question['topic'] = curr_topic

                            # Set the department
                            question['department'] = curr_department

                            # Set the section
                            question['section'] = curr_section
                            question['section_tag'] = curr_section_tag

                            # Set the time
                            question['date'] = frag_date

                            # Set the file it's from.
                            question['hansard_file'] = curr_xml_file

                            # Set the current question.
                            curr_question = question

                            # Add the current paragraph first to maintain order.
                            add_para(curr_para, hansard_debate)
                            curr_para = None

                            # Add question to debate list
                            add_para(question, hansard_debate)

                        # Check if it is just a paragraph of speech.
                        elif curr_tag == "hs_Para":
                            # Ignore certain sections.
                            if curr_section_tag == "hs_2BusinessWODebate":
                                continue

                            # Get the paragraph deets.
                            para = process_para(child)

                            # Set the topic.
                            # para['topic'] = curr_topic

                            # Only set the question if it is the answers section.
                            if curr_section_tag == "hs_3OralAnswers":
                                para['question'] = curr_question

                            # Set the date and the section.
                            para['date'] = frag_date
                            para['section'] = curr_section
                            para['section_tag'] = curr_section_tag
                            # Set the file it's from.
                            para['hansard_file'] = curr_xml_file

                            # Set the current speaker in case next paragraph doesn't specify.
                            if para['member'] is not None:
                                # Add the previous paragraph to the list.
                                add_para(curr_para, hansard_debate)
                                # Update the current paragraph.
                                curr_para = para
                                curr_speaker = para['member']
                            else:
                                # If speaker wasn't recorded, it is the previous speaker.
                                para['member'] = curr_speaker
                                # If this is just a continuation, add the text to the previous para.
                                if curr_para is not None:
                                    curr_para['text'] = "{0} {1}".format(curr_para['text'], para['text'])

                        # Check if it is a quote.
                        elif curr_tag == "hs_brev":
                            quote_text = child.text
                            if curr_para is not None and quote_text is not None:
                                quote_text = quote_text.replace("\n", " ").strip()
                                if re.match(r'[\“\'\"].+[\”\'\"]', quote_text):
                                    curr_para['text'] = "{0} {1}".format(curr_para['text'], quote_text)


                    except Exception as e:
                        print("Problems processing {}".format(child.tag))
                        print(e)
                frag_num += 1
                add_para(curr_para, hansard_debate)

            # Writes this debate to a json file.
            with open("{0}/{1}.json".format(outdir, filename), "w") as out_file:
                json.dump(hansard_debate, out_file)

with open("section_file.json", "w") as section_file:
    json.dump(list(all_sections), section_file)
