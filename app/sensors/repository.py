from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import json

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, mongodb: Session, sensor: schemas.SensorCreate) -> models.Sensor:

    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mongo_sensor = {
        "id": db_sensor.id,
        "name": sensor.name,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "latitude": sensor.latitude,
        "longitude": sensor.longitude,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version
    }
    
    mongodb.collection.insert_one(mongo_sensor)

    return db_sensor

def record_data(redis: Session, db: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    redis.set(sensor_id, json.dumps(data.dict()))
    db_sensordata = data
    return db_sensordata

def get_data(redis: Session,  db: Session, sensor_id: int) -> schemas.Sensor:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    data = redis.get(sensor_id)
    db_sensordata = json.loads(data)
    db_sensordata["name"] = db_sensor.name
    db_sensordata["id"] = db_sensor.id
    return db_sensordata

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(redis: Session, mongodb: Session, latitude: float, longitude: float, radius: int):
    resposta = []
    documents = mongodb.collection.find({"latitude": {"$gte": latitude - radius, "$lte": latitude + radius}, "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}})
    
    for sensor in documents:
        aux = {}
        redis_result = json.loads(redis.get(sensor["id"]))

        aux["id"] = sensor["id"]
        aux["name"] = sensor["name"]
        aux["velocity"] = redis_result["velocity"]
        aux["temperature"] = redis_result["temperature"]
        aux["humidity"] = redis_result["humidity"]
        aux["battery_level"] = redis_result["battery_level"]
        aux["last_seen"] = redis_result["last_seen"]
        
        resposta.append(aux)
    return resposta
    
    