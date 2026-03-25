import asyncio
import os

import httpx
from sqlalchemy import delete

# ВАЖНО: Убедись, что импорты соответствуют путям в этом проекте
from altegio_bot.db import SessionLocal
from altegio_bot.models import MessageTemplate


async def sync():
    token = os.environ.get("WHATSAPP_ACCESS_TOKEN")
    waba_id = os.environ.get("META_WABA_ID")

    if not token or not waba_id:
        print("❌ Ошибка: Не найдены токены в окружении!")
        return

    url = f"https://graph.facebook.com/v20.0/{waba_id}/message_templates"
    print(f"📡 Загружаем шаблоны из Meta (WABA ID: {waba_id})...")

    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        if r.status_code != 200:
            print(f"❌ Ошибка Meta API: {r.text}")
            return
        data = r.json().get("data", [])

    async with SessionLocal() as session:
        async with session.begin():
            # Очищаем старые шаблоны[cite: 11]
            await session.execute(delete(MessageTemplate).where(MessageTemplate.company_id.in_([758285, 1271200])))

            count = 0
            for t in data:
                if t["status"] != "APPROVED":  # Берем только одобренные[cite: 11]
                    continue

                name = t["name"]

                body_text = next((c["text"] for c in t["components"] if c["type"] == "BODY"), "")
                footer_text = next((c["text"] for c in t["components"] if c["type"] == "FOOTER"), "")
                full_body = body_text + (f"\n\n{footer_text}" if footer_text else "")

                # Распределяем по филиалам на основе имени[cite: 11]
                cid = 758285 if "_ka_" in name else 1271200

                # Чистим код шаблона от префиксов и версий[cite: 11]
                code = (
                    name.replace("kitilash_ka_", "").replace("kitilash_ra_", "").replace("_v1", "").replace("_v2", "")
                )

                # Приводим к единому стандарту[cite: 11]
                if code == "record_created_new_client":
                    code = "record_created_new"

                session.add(
                    MessageTemplate(
                        company_id=cid,
                        code=code,
                        language=t["language"],
                        body=full_body,
                        is_active=True,
                    )
                )
                count += 1
                print(f"  ✅ {name} -> {code} ({cid})")

    print(f"🎉 Синхронизация завершена! Добавлено шаблонов: {count}")


if __name__ == "__main__":
    asyncio.run(sync())
