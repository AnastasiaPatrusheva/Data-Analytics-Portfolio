# Данные

Сырые файлы (`content.csv`, `audition.csv`) не хранятся в репозитории из-за размера
(`audition.csv` ~160 МБ). Ниже — схема, чтобы пайплайн был воспроизводим. Положите оба
файла в эту папку (`data/content.csv`, `data/audition.csv`).

## content.csv — справочник произведений (~31 700 строк, без заголовка)

| поле | тип | описание |
|------|-----|----------|
| main_content_id | string | ID произведения |
| main_content_type | string | тип: Audiobook / Book / Comicbook |
| main_content_name | string | название |
| main_content_duration_hours | double | длительность, часы |
| published_topic_title_list | string | список тем/жанров |
| main_author_id | string | автор |

## audition.csv — журнал прослушиваний (~1 000 000 строк, без заголовка)

| поле | тип | описание |
|------|-----|----------|
| usage_geo_id | int | ID гео |
| audition_id | int | ID события (ключ дедупликации) |
| puid | string | ID пользователя |
| usage_platform_ru | string | платформа |
| msk_business_dt_str | string | бизнес-дата, YYYY-MM-DD |
| app_version | string | версия приложения (бывает null) |
| adult_content_flg | bool | флаг взрослого контента |
| hours | double | часы прослушивания |
| hours_sessions_long | double | часы длинных сессий |
| kids_content_flg | bool | флаг детского контента |
| main_content_id | string | ссылка на content |
| usage_geo_id_name | string | город/регион |
| usage_country_name | string | страна |
