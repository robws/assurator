# @title 📂 Upload + Summarize Adobe Analytics Events { display-mode: "form" }
# @markdown Upload a `.chlsj` or Assurance `.json` file. Outputs one-line summaries per event.

from google.colab import files
from IPython.display import display, HTML
import json




def detect_file_type(json_file):
    """
    Determines whether a JSON file is in Adobe Assurance or Charles Proxy format.

    Args:
        json_file (str): Path to the input JSON file.

    Returns:
        str: "assurance" if the file matches Adobe Assurance format,
             "charles" if the file matches Charles Proxy format.

    Raises:
        ValueError: If the file is not valid JSON, unreadable, or does not match known formats.
    """
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)

        if isinstance(data, dict) and "events" in data:
            for event in data["events"]:
                if event.get("payload", {}).get("ACPExtensionEventName") == "Edge Bridge Request":
                    return "assurance"
            raise ValueError("JSON has 'events' but none are Edge Bridge Request (not valid Assurance export)")

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("host") == "hilton.data.adobedc.net":
                    return "charles"
            raise ValueError("JSON is a list, but no entries have host 'hilton.data.adobedc.net' (not valid Charles export)")

        raise ValueError("JSON root is neither an object with 'events' nor a list")
    except json.JSONDecodeError:
        raise ValueError("File is not valid JSON")
    except IOError:
        raise ValueError("File could not be read")


def extract_assurance_edge_bridge_events(json_file, is_debug=False, max_events=None):
    """Extracts Edge Bridge Request events, which represent analytics tracking, from an Assurance JSON export file

       Args:
           json_file: (string) Full file path to Assurance JSON file
           is_debug: (bool) if True, print extra debug information to the console
           max_events: (int) restrict the number of events processed to this number (for testing)

       Returns:
           Dictionary of Edge Bridge Request events with keys "Event N" and values as formatted JSON strings
    """
    with open(json_file, 'r') as f:
        data = json.load(f)

    events = []
    valid_events_count = 0
    total_events_processed = 0

    for index, event in enumerate(data.get("events", [])):
        total_events_processed += 1

        if max_events and valid_events_count >= max_events:
            break  # Limit the number of events for testing

        if event.get("payload", {}).get("ACPExtensionEventName") == "Edge Bridge Request":
            event_data = event.get("payload", {}).get("ACPExtensionEventData", {})

            valid_events_count += 1
            events.append((event.get("timestamp", 0), event_data))

            if is_debug:
                analytics = event_data.get("data", {}).get("__adobe", {}).get("analytics", {})
                context_data = analytics.get("contextData", {})

                if context_data:  # Only print if contextData is not empty
                    print(f"Extracted name/value pairs for event {index + 1} at timestamp {event.get('timestamp', 'Unknown')}:")
                    print_dict_as_fixed_width(context_data)
                else:
                    print(f"No contextData found for event {index + 1} at timestamp {event.get('timestamp', 'Unknown')}")

    if is_debug:
        print(f"Processed {total_events_processed} total events, found {valid_events_count} valid events matching criteria")

    events.sort(key=lambda x: x[0])  # Sort by timestamp
    return {f"Event {i+1}": json.dumps(event[1], indent=4) for i, event in enumerate(events)}



def extract_adobe_events_from_charles(json_file, is_debug=False, max_events=None):
    """
    Extracts Adobe Analytics event objects from a Charles Proxy log file.

    Args:
        json_file (str): Path to the Charles JSON file.
        is_debug (bool): If True, prints debug information.
        max_events (int or None): Maximum number of events to extract.

    Returns:
        List[Dict]: A list of individual Adobe event dictionaries.
    """
    with open(json_file, 'r') as f:
        data = json.load(f)

    events = []
    for index, call in enumerate(data):
        if "hilton.data.adobedc.net" not in call.get("host", ""):
            continue

        try:
            body = call.get("request", {}).get("body", {}).get("text", "")
            call_data = json.loads(body)
            call_events = call_data.get("events", [])

            for event_idx, event in enumerate(call_events):
                events.append(event)

                if is_debug:
                    context = event.get("data", {}).get("__adobe", {}).get("analytics", {}).get("contextData", {})
                    if context:
                        print(f"Call {index+1}, Event {event_idx+1}:")

                        for k, v in context.items():
                            print(f"  {k}: {v}")

                if max_events and len(events) >= max_events:
                    return events

        except (json.JSONDecodeError, TypeError):
            if is_debug:
                print(f"Skipping malformed call at index {index}")

    return events


def parse_launch_event(event):
    xdm = event.get("xdm", {})
    data = event.get("data", {})

    version = xdm.get("application", {}).get("version")
    name = xdm.get("application", {}).get("name")
    os_type = xdm.get("environment", {}).get("operatingSystem")
    os_version = xdm.get("environment", {}).get("operatingSystemVersion")
    device_model = xdm.get("device", {}).get("model")

    return f"Launch: {name} {version}, {os_type} {os_version}, Model: {device_model}"


def parse_track_event(event):
    data = event.get("data", {}).get("__adobe", {}).get("analytics", {})
    context_data = data.get("contextData", {})
    page_name = data.get("pageName") or context_data.get("hm.page.name")
    page_previous = context_data.get("hm.page.previous")
    link_name = data.get("linkName")

    if page_name:
        return f"Page: {page_name}; previous page {page_previous or 'unknown'}"
    elif link_name:
        return f"Link: {link_name}; previous page {page_previous or 'unknown'}"
    else:
        return "Unknown analytics TRACK event"




# Assumes all your original extract_* functions and detect_file_type are already defined

uploaded = files.upload()
input_path = list(uploaded.keys())[0]
try:
    filetype = detect_file_type(input_path)
    print(f"Detected file type: {filetype}")
except ValueError as e:
    print(f"❌ File detection failed: {e}")
    raise

match filetype:
    case "charles":
        events = extract_adobe_events_from_charles(input_path, is_debug=False)
        events = {f"Event {i+1}": json.dumps(e, indent=4) for i, e in enumerate(events)}

    case "assurance":
        events = extract_assurance_edge_bridge_events(input_path, is_debug=False)
    case _:
        raise ValueError(f"Unsupported file type: {filetype}")

# DEBUG: Dump first parsed event structure for inspection
first_event_raw = next(iter(events.values()))
first_event = json.loads(first_event_raw)
second_event_raw = list(events.values())[1]
second_event = json.loads(second_event_raw)

descriptions = []

for event_name, event_json in events.items():
    event = json.loads(event_json)
    xdm = event.get("xdm", {})

    if xdm.get("eventType") == "application.launch":
        desc = parse_launch_event(event)

    elif xdm.get("eventType") == "analytics.track":
        desc  = parse_track_event(event)
    elif xdm.get("eventType") == "application.close":
        desc  = "Application Close"

    else:
        event_type = event.get("xdm", {}).get("eventType", "Unknown")
        desc = f"Unhandled event type: {event_type}"

    descriptions.append(desc)

from IPython.display import display, HTML
display(HTML(f"<textarea rows='30' style='width:100%'>{chr(10).join(descriptions)}</textarea>"))
