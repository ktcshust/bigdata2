import csv
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd

# Đường dẫn đến tệp serviceAccountKey.json
cred = credentials.Certificate('bigdata-eec24-firebase-adminsdk-n4a82-94ef80532e.json')

if not firebase_admin._apps:
    # Khởi tạo ứng dụng Firebase
    firebase_admin.initialize_app(cred)

df = pd.read_csv('top_20_ratio.csv')

# Đường dẫn đến tệp CSV
data = df.to_dict(orient='records')

db = firestore.client()
collection_ref = db.collection('top_20_ratio')

for record in data:
    # Tìm tất cả các tài liệu trong collection có trường 'name' trùng với 'name' từ CSV
    query = collection_ref.where('name', '==', record['name'])
    docs = query.stream()

    # Cập nhật trường 'count' của các tài liệu tìm thấy
    for doc in docs:
        doc.reference.update({'pledged_goal_ratio ': record['pledged_goal_ratio']})

