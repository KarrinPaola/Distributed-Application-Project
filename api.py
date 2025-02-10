from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

class Item(BaseModel):
    key: str
    value: any

def create_api(db):
    app = FastAPI()

    @app.get("/")
    def home():
        return {"message": "PickleDB REST API is running!"}

    @app.get("/get/{key}")
    def get_item(key: str):
        value = db.get(key)
        if value is None:
            raise HTTPException(status_code=404, detail="Key not found")
        return {"key": key, "value": value}

    @app.post("/set")
    def set_item(item: Item):
        db.set(item.key, item.value)
        db.save()
        return {"message": "Data saved", "key": item.key, "value": item.value}

    @app.delete("/delete/{key}")
    def delete_item(key: str):
        success = db.remove(key)
        if not success:
            raise HTTPException(status_code=404, detail="Key not found")
        db.save()
        return {"message": "Key deleted", "key": key}

    @app.get("/all/")
    def get_all_keys():
        return {"keys": db.all()}

    return app
