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

df1 = pd.read_csv('top_20_ratio.csv')

# Đường dẫn đến tệp CSV
data1 = df1.to_dict(orient='records')

db = firestore.client()
collection_ref1 = db.collection('top_20_ratio')

for record in data1:
    # Tìm tất cả các tài liệu trong collection có trường 'name' trùng với 'name' từ CSV
    query1 = collection_ref1.where('name', '==', record['name'])
    docs1 = query1.stream()

    # Cập nhật trường 'count' của các tài liệu tìm thấy
    for doc in docs1:
        doc.reference.update({'pledged_goal_ratio ': record['pledged_goal_ratio']})

df2 = pd.read_csv('top_20_country.csv')

# Đường dẫn đến tệp CSV
data2 = df2.to_dict(orient='records')

collection_ref2 = db.collection('top_20_country')

for record in data2:
    # Tìm tất cả các tài liệu trong collection có trường 'name' trùng với 'name' từ CSV
    query2 = collection_ref2.where('country', '==', record['country'])
    docs2 = query2.stream()

    # Cập nhật trường 'count' của các tài liệu tìm thấy
    for doc in docs2:
        doc.reference.update({'count': record['count']})

df3 = pd.read_csv('top_20_category.csv')

# Đường dẫn đến tệp CSV
data3 = df3.to_dict(orient='records')


collection_ref3 = db.collection('top_20_category')

for record in data3:
    # Tìm tất cả các tài liệu trong collection có trường 'name' trùng với 'name' từ CSV
    query3 = collection_ref3.where('category', '==', record['category'])
    docs3 = query3.stream()

    # Cập nhật trường 'count' của các tài liệu tìm thấy
    for doc in docs3:
        doc.reference.update({'pledged': record['pledged']})

