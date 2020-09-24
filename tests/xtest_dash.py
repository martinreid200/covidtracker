from dash.testing.application_runners import import_app
import re

def test_two(dash_duo):
    app = import_app("app")
    dash_duo.start_server(app)


    #link1 = "http://192.168.1.170:5200/"
    #dash_duo.wait_for_page(url=link1)
    #dash_duo.take_snapshot("map1")



    #map = dash_duo.find_element("#map")
    #dash_duo.wait_for_text_to_equal("h1", "Hello Dash", timeout=4)
    #dash_duo.wait_for_element_by_id("map", timeout=10)

    #print("\nFound map",map)

    #dash_duo.click_at_coord_fractions(map, 0.75, 0.5)

    #items = str(app.layout).split(",")
    #links = [x for x in items if x.startswith(" href='/link")]
    #for rawlink in links:
    #    href = re.findall(r"'(.*?)'", rawlink)
    #    link = "http://127.0.0.1:8050"+href[0]
    #    print("Loading",link)
    #    dash_duo.wait_for_page(url=link)
    #    print("Loaded",link)

    #assert len(dash_duo.find_elements('map')) > 0 
    #dash_duo.take_snapshot("test_two")
    #dash_duo.percy_snapshot("test_one")
    
    assert dash_duo.get_logs() == [], "browser console should contain no error"

    
     