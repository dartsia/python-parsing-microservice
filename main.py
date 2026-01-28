from fastapi import FastAPI, UploadFile, File, HTTPException
from docling.document_converter import DocumentConverter
import pandas as pd
import re
from pathlib import Path
import tempfile
import os

os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
os.environ["HF_HUB_DISABLE_SYMLINKS"] = "1"

app = FastAPI()

def extract_signatures(text):
    signatures = {}

    dean_pattern = r"Декан факультету.*?комп'ютерних технологій.*?(доц\.\s*[A-Я]\.[A-Я]\.\s*[A-Яа-я]+)"

    head_pattern = r"Завідувач.*?кафедри системного проектування.*?(доц\.\s*[A-Я]\.[A-Я]\.\s*[A-Яа-я]+)"
    
    dean_match = re.search(dean_pattern, text, re.S | re.I)
    if dean_match:
        signatures['dean'] = dean_match.group(1).strip()
    else:
        dean_fallback = re.search(r"доц\.\s*Ю\.М\.\s*Фургала", text)
        if dean_fallback:
            signatures['dean'] = dean_fallback.group(0)
        
    head_match = re.search(head_pattern, text, re.S | re.I)
    if head_match:
        signatures['departmentHead'] = head_match.group(1).strip()
    else:
        head_fallback = re.search(r"доц\.\s*Р\.Я\.\s*Шувар", text)
        if head_fallback:
            signatures['departmentHead'] = head_fallback.group(0)
    
    return signatures

def parse_discipline_row(row_data, semester):
    specialty_code = None
    spec_col_idx = None
    
    for idx, val in enumerate(row_data):
        if pd.notna(val):
            match = re.search(r'(\d{3,})-(\d)', str(val))
            if match:
                specialty_code = match.group(0)
                specialty = match.group(1)
                course = int(match.group(2))
                spec_col_idx = idx
                break
    
    if not specialty_code:
        return None

    name_parts = []
    for idx in range(spec_col_idx):
        val = row_data[idx]
        if pd.notna(val) and str(val).strip():
            val_str = str(val).strip()
            if not re.match(r'^\d+$', val_str) and val_str not in ["Ел.", "Ел"]:
                name_parts.append(val_str)
    
    name = " ".join(name_parts)
    name = re.sub(r'\s+', ' ', name).strip()
    
    if not name or len(name) < 3:
        return None

    faculty = str(row_data[spec_col_idx - 1]) if spec_col_idx > 0 and pd.notna(row_data[spec_col_idx - 1]) else "Ел."

    data_start_idx = spec_col_idx + 1
    
    def get_int(idx, default=0):
        if len(row_data) > idx and pd.notna(row_data[idx]):
            nums = re.findall(r'\d+', str(row_data[idx]))
            return int(nums[0]) if nums else default
        return default
    
    def get_split(idx):
        if len(row_data) > idx and pd.notna(row_data[idx]):
            nums = re.findall(r'\d+', str(row_data[idx]))
            return (int(nums[0]) if len(nums) > 0 else 0, 
                    int(nums[1]) if len(nums) > 1 else 0)
        return (0, 0)
    
    students = get_int(data_start_idx)
    l_ft, l_pt = get_split(data_start_idx + 1)
    p_ft, p_pt = get_split(data_start_idx + 2)
    lb_ft, lb_pt = get_split(data_start_idx + 3)
    c_ft, c_pt = get_split(data_start_idx + 4)
    e_ft, e_pt = get_split(data_start_idx + 5)
    z_ft, z_pt = get_split(data_start_idx + 6)
    
    return {
        "name": name,
        "faculty": faculty,
        "specialty": specialty,
        "course": course,
        "semester": semester,
        "students": students,
        "lecturesFullTime": l_ft, "lecturesPartTime": l_pt,
        "practicalsFullTime": p_ft, "practicalsPartTime": p_pt,
        "labsFullTime": lb_ft, "labsPartTime": lb_pt,
        "consultationsFullTime": c_ft, "consultationsPartTime": c_pt,
        "examsFullTime": e_ft, "examsPartTime": e_pt,
        "creditsFullTime": z_ft, "creditsPartTime": z_pt,
        "controlWorks": get_int(data_start_idx + 7),
        "courseWorks": get_int(data_start_idx + 8),
        "thesisWorks": get_int(data_start_idx + 9),
        "pedPractice": get_int(data_start_idx + 10),
        "educationalPractice": get_int(data_start_idx + 11),
        "productionPractice": get_int(data_start_idx + 12),
        "stateExams": get_int(data_start_idx + 13),
        "postgraduateStudies": get_int(data_start_idx + 14),
        "other": get_int(data_start_idx + 15)
    }

print("Ініціалізація Docling (завантаження моделей)...")
converter = DocumentConverter()
print("Docling готовий до роботи")

@app.post("/parse")
async def parse_document(file: UploadFile = File(...)):
    disciplines = []

    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        print(f"Парсинг файлу: {file.filename}")

        result = converter.convert(tmp_path)

        full_text = result.document.export_to_markdown()
        
        print(f"Знайдено таблиць: {len(result.document.tables)}")

        current_semester = 1
        
        for table_idx, table in enumerate(result.document.tables):
            df = table.export_to_dataframe()
            
            print(f"\nТаблиця {table_idx + 1}: {df.shape}")

            for idx, row in df.iterrows():
                row_data = row.values.tolist()
                row_text = " ".join([str(v) for v in row_data if pd.notna(v)])

                if "Всього" in row_text and ("семестр" in row_text or "І" in row_text.upper()):
                    current_semester = 2
                    print(f"Перемикання на 2-й семестр")
                    continue

                if any(kw in row_text for kw in ["Дисципліни", "Факультет", "Спеціальність", 
                                                   "студентів", "денне", "заочне", "Лекції", "Практ"]):
                    continue

                discipline = parse_discipline_row(row_data, current_semester)
                
                if discipline:
                    print(f"→ {discipline['name']} | {discipline['specialty']}-{discipline['course']} | Семестр {current_semester}")
                    disciplines.append(discipline)
        
        print(f"\nВсього дисциплін: {len(disciplines)}")

        meta = extract_signatures(full_text)
        
        return {"metadata": meta, "disciplines": disciplines}
    
    except Exception as e:
        print(f"Помилка: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        Path(tmp_path).unlink(missing_ok=True)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)