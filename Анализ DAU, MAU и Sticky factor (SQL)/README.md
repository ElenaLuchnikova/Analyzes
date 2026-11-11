[Анализ в Google Colab](https://colab.research.google.com/drive/1m6nfJw3m-7Aiy8vOX81nQ6E7PpmSNKXZ#scrollTo=0eecykPcysni)

**Описание задачи**

У нас есть данные о продажах интернет-магазина косметики и бытовой химии за 2023-2025 годы. Известно, что с января по июнь 2024 проводились кампании по привлечению клиентов, в том числе скидки на повторную покупку с ограничением по времени - первая неделя.

Рассчитаем и проведем анализ DAU, MAU и Sticky factor для интернет-магазина косметики.

Возьмем период с 01.01.2024 по 31.12.2024 и рассчитаем следующие показатели:

* количество уникальных активных пользователей за день (DAU)

* количество уникальных активных пользователей за месяц (MAU)

* отношение DAU к MAU (Sticky factor)

DAU (Daily Active Users) — это метрика, которая показывает количество уникальных пользователей, которые зашли в продукт (приложение, сайт, сервис) в течение одного дня. Простыми словами: сколько разных людей пользовались нашим продуктом за сутки.

MAU (Monthly Active Users) — количество уникальных пользователей, которые проявили активность в продукте в течение одного календарного месяца.

Sticky Factor (коэффциент липкости, вовлечённость) — метрика, показывающая, какая доля пользователей возвращается в продукт регулярно. Чаще всего считается как отношение DAU к MAU. В идеале должен стремиться к 1

**Как было реализовано решение задачи**

Первым этапом я рассчитала DAU, MAU и Sticky factor с помощью [SQL](https://github.com/ElenaLuchnikova/Analyzes/blob/main/%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D0%B7%20DAU%2C%20MAU%20%D0%B8%20Sticky%20factor%20(SQL)/Code.sql). Затем построила  [график динамики DAU](https://github.com/ElenaLuchnikova/Analyzes/blob/main/%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D0%B7%20DAU%2C%20MAU%20%D0%B8%20Sticky%20factor%20(SQL)/%D0%94%D0%B8%D0%BD%D0%B0%D0%BC%D0%B8%D0%BA%D0%B0%20DAU.jpg), [график динамики MAU и DAU](https://github.com/ElenaLuchnikova/Analyzes/blob/main/%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D0%B7%20DAU%2C%20MAU%20%D0%B8%20Sticky%20factor%20(SQL)/%D0%94%D0%B8%D0%BD%D0%B0%D0%BC%D0%B8%D0%BA%D0%B0%20DAU%20%D0%B8%20MAU.jpg) и [график динамики Sticky factor](https://github.com/ElenaLuchnikova/Analyzes/blob/main/%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D0%B7%20DAU%2C%20MAU%20%D0%B8%20Sticky%20factor%20(SQL)/%D0%94%D0%B8%D0%BD%D0%B0%D0%BC%D0%B8%D0%BA%D0%B0%20Sticky%20Factor.jpg). На основании  динамики исследуемых метрик я сделала [выводы и рекомендации](https://github.com/ElenaLuchnikova/Analyzes/blob/main/%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D0%B7%20DAU%2C%20MAU%20%D0%B8%20Sticky%20factor%20(SQL)/Insights.md).
