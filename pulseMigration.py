import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd

# Initialize Firebase
cred = credentials.Certificate("btm-data-collector-firebase-adminsdk-qg75l-5191f62c21.json")  # Replace with your service account key path
firebase_admin.initialize_app(cred)
db = firestore.client()

# Collection to export
collection_name = "Individual_Purse"

# Define a function to retrieve and export data
def export_individual_purse_to_csv():
    # Query Firestore to get all documents from the collection
    docs = db.collection(collection_name).stream()

    # List to store all document data
    data = []

    for doc in docs:
        doc_data = doc.to_dict()

        # Extract data for each form
        form_data = {
            "registrationNumber": doc_data.get("registrationNumber", ""),
            "firstName": doc_data.get("firstName", ""),
            "middleName": doc_data.get("middleName", ""),
            "lastName": doc_data.get("lastName", ""),
            "gender": doc_data.get("gender", ""),
            "dateOfBirth": doc_data.get("dateOfBirth", ""),
            "maritalStatus": doc_data.get("maritalStatus", ""),
            "education": doc_data.get("education", ""),
            "monthlyIncome": doc_data.get("monthlyIncome", ""),
            "monthlyIncomeCurrency": doc_data.get("monthlyIncomeCurrency", ""),
            "monthlyExpenditure": doc_data.get("monthlyExpenditure", ""),
            "monthlyExpenditureCurrency": doc_data.get("monthlyExpenditureCurrency", ""),
            "householdSize": doc_data.get("householdSize", ""),
            "incomeSources": ", ".join(doc_data.get("incomeSources", [])) if isinstance(doc_data.get("incomeSources"), list) else "",
            "ownershipStatus": doc_data.get("ownershipStatus", ""),
            "farmSize": doc_data.get("farmSize", ""),
            "primaryDuties": ", ".join(doc_data.get("primaryDuties", [])) if isinstance(doc_data.get("primaryDuties"), list) else "",
            "farmDecisionMaker": doc_data.get("farmDecisionMaker", ""),
            "salesMethods": ", ".join(doc_data.get("salesMethods", [])) if isinstance(doc_data.get("salesMethods"), list) else "",
            "pulseIncomeUsage": doc_data.get("pulseIncomeUsage", ""),
            "trainingProviders": ", ".join(doc_data.get("trainingProviders", [])) if isinstance(doc_data.get("trainingProviders"), list) else "",
            "challenges": doc_data.get("challenges", ""),
            "memberOfPulseGroup": doc_data.get("memberOfPulseGroup", ""),
            "paymentPhases": doc_data.get("paymentPhases", ""),
            "preferredPaymentMethods": ", ".join(doc_data.get("preferredPaymentMethods", [])) if isinstance(doc_data.get("preferredPaymentMethods"), list) else "",
            "pulseLoanProviders": ", ".join(doc_data.get("pulseLoanProviders", [])) if isinstance(doc_data.get("pulseLoanProviders"), list) else "",
            "pulseTechTools": ", ".join(doc_data.get("pulseTechTools", [])) if isinstance(doc_data.get("pulseTechTools"), list) else "",
            "futureGoals": ", ".join(doc_data.get("futureGoals", [])) if isinstance(doc_data.get("futureGoals"), list) else "",
            "supportNeeded": ", ".join(doc_data.get("supportNeeded", [])) if isinstance(doc_data.get("supportNeeded"), list) else "",
            "nationalIdNumber": doc_data.get("nationalIdNumber", ""),
            "issueDate": doc_data.get("issueDate", ""),
            "expiryDate": doc_data.get("expiryDate", ""),
            "placeOfIssue": doc_data.get("placeOfIssue", ""),
            "issuedBy": doc_data.get("issuedBy", ""),
            "street": doc_data.get("street", ""),
            "houseNumber": doc_data.get("houseNumber", ""),
            "postalBox": doc_data.get("postalBox", ""),
            "city": doc_data.get("city", ""),
            "district": doc_data.get("district", ""),
            "region": doc_data.get("region", ""),
            "country": doc_data.get("country", "Tanzania"),
            "phoneNumber": doc_data.get("phoneNumber", ""),
            "secondPhoneNumber": doc_data.get("secondPhoneNumber", ""),
            "user_name": doc_data.get("user_name", ""),
            "user_place": doc_data.get("user_place", ""),
            "user_role": doc_data.get("user_role", ""),
            "user_work": doc_data.get("user_work", ""),
            "user_id": doc_data.get("user_id", ""),
            "form_type": doc_data.get("form_type", ""),
            "submitted_date": doc_data.get("submitted_date", ""),
        }

        # Append form_data to the data list
        data.append(form_data)

    # Create a DataFrame from the data list
    df = pd.DataFrame(data)

    # Export DataFrame to CSV
    df.to_csv("individual_purse_data.csv", index=False)
    print("Data exported to individual_purse_data.csv")

# Run the export function
export_individual_purse_to_csv()