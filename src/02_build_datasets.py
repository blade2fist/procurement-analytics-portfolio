from pathlib import Path
import re
import numpy as np
import pandas as pd


# -----------------------------------------------------------------------------
# 1. Пути
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_FILE = BASE_DIR / "data_raw" / "contracts_ra.csv"
OUTPUT_DIR = BASE_DIR / "data_processed"
OUTPUT_DIR.mkdir(exist_ok=True)


# -----------------------------------------------------------------------------
# 2. Чтение сырого файла
# -----------------------------------------------------------------------------
df = pd.read_csv(
    RAW_FILE,
    sep=";",
    encoding="cp1251",
    dtype=str
)


# -----------------------------------------------------------------------------
# 3. Переименование столбцов
# -----------------------------------------------------------------------------
COLUMN_MAP = {
    "Номер реестровой записи контракта": "registry_id",
    "Заказчик: наименование": "customer_name",
    "Заказчик: ИНН": "customer_inn",
    "Заказчик: КПП": "customer_kpp",
    "Уровень бюджета": "budget_level",
    "Источник финансирования контракта: наименование бюджета": "budget_name",
    "Источник финансирования контракта: наименование/вид внебюджетных средств": "extra_budget_source",
    "Способ размещения заказа": "purchase_method",
    "Номер извещения о проведени торгов": "notice_number",
    "Дата подведения результатов определения поставщика (подрядчика, исполнителя)": "supplier_selection_result_date",
    "Реквизиты документа, подтверждающего основание заключения контракта": "basis_document_details",
    "Контракт: дата": "contract_date",
    "Контракт: номер": "contract_number",
    "Предмет контракта": "contract_subject",
    "Цена контракта": "contract_price",
    "Код бюджетной классификации": "budget_code",
    "КОСГУ": "kosgu",
    "КВР": "kvr",
    "Идентификационный код закупки (ИКЗ)": "ikz",
    "Объект закупки: наименование товаров, работ, услуг": "item_name",
    "Объект закупки: код позиции": "item_code",
    "Объект закупки: цена за единицу, рублей": "item_unit_price",
    "Объект закупки: количество поставленных товаров, выполненных работ, оказанных услуг": "item_quantity",
    "Объект закупки: сумма, рублей": "item_amount",
    "Информация о поставщиках (исполнителях, подрядчиках) по контракту: наименование юридического лица (ф.и.о. физического лица)": "supplier_name",
    "Информация о поставщиках (исполнителях, подрядчиках) по контракту: ИНН": "supplier_inn",
    "Информация о поставщиках (исполнителях, подрядчиках) по контракту: КПП": "supplier_kpp",
    "Дата последнего изменения записи": "last_update_datetime",
    "Дата исполнения контракта: по контракту": "contract_execution_date",
}

df = df.rename(columns=COLUMN_MAP)


# -----------------------------------------------------------------------------
# 4. Проверка обязательных столбцов
# -----------------------------------------------------------------------------
required_columns = [
    "registry_id",
    "customer_name",
    "contract_date",
    "contract_price",
    "item_name",
    "supplier_name",
]

missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"В файле не найдены обязательные столбцы: {missing_columns}")


# -----------------------------------------------------------------------------
# 5. Функции очистки
# -----------------------------------------------------------------------------
def clean_text(value):
    if pd.isna(value):
        return np.nan

    value = str(value)
    value = value.replace("\xa0", " ")
    value = value.strip()
    value = value.strip("'")
    value = value.strip()
    value = re.sub(r"\s+", " ", value)

    if value == "" or value.lower() == "nan":
        return np.nan

    return value


def clean_number(value):
    if pd.isna(value):
        return np.nan

    value = str(value)
    value = value.replace("\xa0", " ")
    value = value.strip()
    value = value.strip("'")
    value = value.strip()
    value = value.replace(" ", "")
    value = value.replace(",", ".")

    if value == "" or value.lower() == "nan":
        return np.nan

    try:
        return float(value)
    except ValueError:
        return np.nan


# -----------------------------------------------------------------------------
# 6. Чистка текстовых столбцов
# -----------------------------------------------------------------------------
for col in df.columns:
    df[col] = df[col].map(clean_text)


# -----------------------------------------------------------------------------
# 7. Числовые поля
# -----------------------------------------------------------------------------
numeric_columns = [
    "contract_price",
    "item_unit_price",
    "item_quantity",
    "item_amount",
]

for col in numeric_columns:
    if col in df.columns:
        df[col] = df[col].map(clean_number)


# -----------------------------------------------------------------------------
# 8. Даты
# -----------------------------------------------------------------------------
date_columns = [
    "supplier_selection_result_date",
    "contract_date",
    "contract_execution_date",
]

for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], format="%d.%m.%Y", errors="coerce")

if "last_update_datetime" in df.columns:
    df["last_update_datetime"] = pd.to_datetime(
        df["last_update_datetime"],
        format="%d.%m.%Y %H:%M",
        errors="coerce"
    )


# -----------------------------------------------------------------------------
# 9. contracts
# -----------------------------------------------------------------------------
contract_columns = [
    "registry_id",
    "customer_name",
    "customer_inn",
    "customer_kpp",
    "budget_level",
    "budget_name",
    "extra_budget_source",
    "purchase_method",
    "notice_number",
    "supplier_selection_result_date",
    "basis_document_details",
    "contract_date",
    "contract_number",
    "contract_subject",
    "contract_price",
    "budget_code",
    "kosgu",
    "kvr",
    "ikz",
    "supplier_name",
    "supplier_inn",
    "supplier_kpp",
    "last_update_datetime",
    "contract_execution_date",
]

contracts = (
    df[contract_columns]
    .drop_duplicates(subset=["registry_id"])
    .sort_values(["contract_date", "registry_id"], na_position="last")
    .reset_index(drop=True)
)

contracts.insert(0, "contract_id", range(1, len(contracts) + 1))


# -----------------------------------------------------------------------------
# 10. contract_items
# -----------------------------------------------------------------------------
contract_items = (
    df[
        [
            "registry_id",
            "item_name",
            "item_code",
            "item_unit_price",
            "item_quantity",
            "item_amount",
        ]
    ]
    .copy()
    .reset_index(drop=True)
)

contract_items.insert(0, "item_id", range(1, len(contract_items) + 1))


# -----------------------------------------------------------------------------
# 11. customers
# -----------------------------------------------------------------------------
customers = (
    df[["customer_name", "customer_inn", "customer_kpp"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

customers.insert(0, "customer_id", range(1, len(customers) + 1))


# -----------------------------------------------------------------------------
# 12. suppliers
# -----------------------------------------------------------------------------
suppliers = (
    df[["supplier_name", "supplier_inn", "supplier_kpp"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

suppliers.insert(0, "supplier_id", range(1, len(suppliers) + 1))


# -----------------------------------------------------------------------------
# 13. Сохранение
# -----------------------------------------------------------------------------
contracts.to_csv(OUTPUT_DIR / "contracts.csv", index=False, encoding="utf-8-sig")
contract_items.to_csv(OUTPUT_DIR / "contract_items.csv", index=False, encoding="utf-8-sig")
customers.to_csv(OUTPUT_DIR / "customers.csv", index=False, encoding="utf-8-sig")
suppliers.to_csv(OUTPUT_DIR / "suppliers.csv", index=False, encoding="utf-8-sig")


# -----------------------------------------------------------------------------
# 14. Контрольные проверки
# -----------------------------------------------------------------------------
print("=" * 80)
print("ГОТОВО: ДАТАСЕТЫ СОБРАНЫ")
print("=" * 80)
print(f"contracts shape: {contracts.shape}")
print(f"contract_items shape: {contract_items.shape}")
print(f"customers shape: {customers.shape}")
print(f"suppliers shape: {suppliers.shape}")
print()

print("ПРОВЕРКИ")
print("-" * 80)
print(f"Уникальных registry_id в contracts: {contracts['registry_id'].nunique()}")
print(f"Строк в contract_items: {len(contract_items)}")
print(f"Пропусков в registry_id (contracts): {contracts['registry_id'].isna().sum()}")
print(f"Пропусков в item_name (contract_items): {contract_items['item_name'].isna().sum()}")
print()
print(f"Файлы сохранены в папку: {OUTPUT_DIR}")