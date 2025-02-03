from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient('mongodb://root:root@localhost:27017/')
db = client['store']

@app.route('/user/<int:userID>', methods=['GET'])  # Force Flask to use int
def get_user_products(userID):
    print("Looking for user with ID:", userID)  # Debugging
    user = db.users.find_one({"userID": userID})  # Ensure userID is an int

    if user:
        print("User found:", user)
        products = list(db.products.find({"id": {"$in": user["clothID"]}}))

        # Convert ObjectId to string for JSON serialization
        for product in products:
            product["_id"] = str(product["_id"])

        print("Products found:", products)
        return jsonify({"products": products})
    else:
        print("User not found!")
        return jsonify({"error": "User not found"}), 404

# ✅ Bonus Query 2: Get products of the user AND their friends/collaborators
@app.route('/user/<int:userID>/friends-products', methods=['GET'])
def get_friends_products(userID):
    user = db.users.find_one({"userID": userID})

    if not user:
        return jsonify({"error": "User not found"}), 404

    # Get the IDs of friends and collaborators
    friends_and_collaborators = user.get("friends", []) + user.get("collaborators", [])

    # Get all users (self + friends + collaborators)
    all_users = [userID] + friends_and_collaborators
    all_cloth_ids = set()

    for uid in all_users:
        friend = db.users.find_one({"userID": uid})
        if friend:
            cloth_ids = friend["clothID"] if isinstance(friend["clothID"], list) else [friend["clothID"]]
            all_cloth_ids.update(cloth_ids)

    # Get all products from MongoDB
    products = list(db.products.find({"id": {"$in": list(all_cloth_ids)}}))

    for product in products:
        product["_id"] = str(product["_id"])

    return jsonify({"products": products})

# ✅ Bonus Query 3: Recommend a product based on friends' purchases
@app.route('/user/<int:userID>/recommend', methods=['GET'])
def recommend_product(userID):
    user = db.users.find_one({"userID": userID})

    if not user:
        return jsonify({"error": "User not found"}), 404

    # Get the IDs of friends and collaborators
    friends_and_collaborators = user.get("friends", []) + user.get("collaborators", [])

    owned_cloth_ids = set(user["clothID"] if isinstance(user["clothID"], list) else [user["clothID"]])
    possible_recommendations = set()

    for uid in friends_and_collaborators:
        friend = db.users.find_one({"userID": uid})
        if friend:
            cloth_ids = friend["clothID"] if isinstance(friend["clothID"], list) else [friend["clothID"]]
            possible_recommendations.update(cloth_ids)

    # Find items that friends have, but the user doesn't
    recommendations = possible_recommendations - owned_cloth_ids

    if recommendations:
        recommended_product = db.products.find_one({"id": list(recommendations)[0]})
        if recommended_product:
            recommended_product["_id"] = str(recommended_product["_id"])
            return jsonify({"recommended_product": recommended_product})

    return jsonify({"message": "No recommendations available"})

# ✅ Bonus Query 4: Recommend the most popular unowned product if all collaborator products are owned
@app.route('/user/<int:userID>/popular-recommend', methods=['GET'])
def recommend_popular_product(userID):
    user = db.users.find_one({"userID": userID})

    if not user:
        return jsonify({"error": "User not found"}), 404

    collaborators = user.get("collaborators", [])
    owned_cloth_ids = set(user["clothID"] if isinstance(user["clothID"], list) else [user["clothID"]])

    # Collect all products owned by collaborators
    collaborator_products = set()
    for uid in collaborators:
        collaborator = db.users.find_one({"userID": uid})
        if collaborator:
            cloth_ids = collaborator["clothID"] if isinstance(collaborator["clothID"], list) else [collaborator["clothID"]]
            collaborator_products.update(cloth_ids)

    # Check if the user already owns all collaborator products
    if collaborator_products.issubset(owned_cloth_ids):
        # Find the most popular unowned product
        pipeline = [
            {"$match": {"id": {"$nin": list(owned_cloth_ids)}}},
            {"$group": {"_id": "$id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        popular_product = list(db.products.aggregate(pipeline))

        if popular_product:
            recommended_product = db.products.find_one({"id": popular_product[0]["_id"]})
            if recommended_product:
                recommended_product["_id"] = str(recommended_product["_id"])
                return jsonify({"recommended_product": recommended_product})

    return jsonify({"message": "No recommendations available"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
