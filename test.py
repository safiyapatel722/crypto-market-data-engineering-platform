import requests
import pytesseract
from pdf2image import convert_from_bytes

base_url = "https://ceomaharashtra.com/wp-content/uploads/"

for i in range(1,134):
    pdf_num = str(i).zfill(3)
    url = f"{base_url}{pdf_num}.pdf"

    try:
        r = requests.get(url, timeout=10)
        images = convert_from_bytes(r.content, first_page=1, last_page=10)

        text = pytesseract.image_to_string(images[0])

        if "413004" in text:
            print("Found in PDF:", pdf_num)

    except:
        pass