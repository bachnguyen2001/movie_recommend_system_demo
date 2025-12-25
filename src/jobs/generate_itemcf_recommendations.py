from src.serving.itemcf_inference import generate_itemcf_recommendations
from src.serving.write_recommendations import write_recommendations

def main():
    df = generate_itemcf_recommendations()
    write_recommendations(df, model_version="itemcf_v1")

if __name__ == "__main__":
    main()
