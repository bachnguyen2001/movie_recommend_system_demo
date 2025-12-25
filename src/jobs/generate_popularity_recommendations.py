from src.serving.popularity_inference import get_popularity_recommendations
from src.serving.write_recommendations import write_recommendations

def main():
    df = get_popularity_recommendations()
    write_recommendations(df, model_version="popularity_v1")

if __name__ == "__main__":
    main()
