from pathlib import Path
import pandas as pd


# 1. Путь к файлу
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_FILE = BASE_DIR / "data_raw" / "contracts_ra.csv"


# 2. Читаем CSV
df = pd.read_csv(
    RAW_FILE,
    sep=";",
    encoding="cp1251",
    dtype=str
)


# 3. Название ключевого столбца
contract_id_col = "Номер реестровой записи контракта"


# 4. Чистим идентификатор контракта только для профилирования
df[contract_id_col] = (
    df[contract_id_col]
    .astype(str)
    .str.strip()
    .str.strip("'")
    .str.strip()
)


# 5. Базовая информация
print("=" * 80)
print("ПРОФИЛЬ СЫРОГО ФАЙЛА")
print("=" * 80)
print(f"Путь к файлу: {RAW_FILE}")
print(f"Количество строк: {df.shape[0]}")
print(f"Количество столбцов: {df.shape[1]}")
print()


# 6. Список столбцов
print("=" * 80)
print("СПИСОК СТОЛБЦОВ")
print("=" * 80)
for i, col in enumerate(df.columns, start=1):
    print(f"{i:02d}. {col}")
print()


# 7. Уникальные контракты
unique_contracts = df[contract_id_col].nunique(dropna=True)
duplicate_rows = df.shape[0] - unique_contracts

print("=" * 80)
print("ПРОВЕРКА ПО КОНТРАКТАМ")
print("=" * 80)
print(f"Уникальных контрактов: {unique_contracts}")
print(f'Повторяющихся строк по логике "один контракт - много позиций": {duplicate_rows}')
print()


# 8. Самые часто повторяющиеся контракты
print("=" * 80)
print("ТОП-10 КОНТРАКТОВ ПО ЧИСЛУ СТРОК")
print("=" * 80)
print(df[contract_id_col].value_counts(dropna=False).head(10))
print()


# 9. Доля пропусков
missing_share = (df.isna().mean() * 100).sort_values(ascending=False)

print("=" * 80)
print("ТОП-10 СТОЛБЦОВ С ПРОПУСКАМИ (%)")
print("=" * 80)
print(missing_share.head(10).round(2))
print()


# 10. Пример первых строк
print("=" * 80)
print("ПЕРВЫЕ 5 СТРОК")
print("=" * 80)
print(df.head())
print()