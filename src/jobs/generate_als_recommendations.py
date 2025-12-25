from src.serving.als_inference import generate_als_recommendations_pandas
from src.serving.write_recommendations import write_recommendations

def main():
    df = generate_als_recommendations_pandas()
    write_recommendations(df, model_version="als_v1")

if __name__ == "__main__":
    main()
