import sqlite3
import pandas as pd

# Loads CSV files into DataFrames to be used for queries
df_results = pd.read_csv('C:/Users/lewis/Downloads/Coding/2024/LewisChallengeDataEngineerPython/results.csv') # Path to csv files
df_shootouts = pd.read_csv('C:/Users/lewis/Downloads/Coding/2024/LewisChallengeDataEngineerPython/shootouts.csv')
df_goalscorers = pd.read_csv('C:/Users/lewis/Downloads/Coding/2024/LewisChallengeDataEngineerPython/goalscorers.csv')


# Set pandas display options to show all rows and columns, this is due to pandas defaulting to limit data output, so this is required to make sure everything is displayed
pd.set_option('display.max_rows', None)  # Shows all rows
pd.set_option('display.max_columns', None)  # Shows all columns
pd.set_option('display.width', None)  # No line width limit

# Connect to SQLite database
with sqlite3.connect('Database.db') as connection: #with used so no connection.close() needs to be called
    
    # Write the DataFrames to SQL tables
    df_results.to_sql('results_table', connection, if_exists='replace', index=False)
    df_shootouts.to_sql('shootouts', connection, if_exists='replace', index=False)
    df_goalscorers.to_sql('goalscorers', connection, if_exists='replace', index=False)

    # Query 1: Average Goals per Game that is between 1900 and 2000
    query_avg_goals = """
    SELECT 
        AVG(home_score + away_score) AS avg_goals_per_game
    FROM 
        results_table
    WHERE 
        date BETWEEN '1900-01-01' AND '2000-12-31';
    """
    avg_goals = pd.read_sql_query(query_avg_goals, connection)
    print("Average Goals per Game (1900-2000):")
    print(avg_goals)  

    # Query 2: Count of Shootout Wins by Country
    query_shootout_wins = """
    SELECT 
        winner, COUNT(*) AS shootout_wins
    FROM 
        shootouts
    GROUP BY 
        winner
    ORDER BY 
        winner ASC;
    """

    shootout_wins = pd.read_sql_query(query_shootout_wins, connection)
    print("Shootout Wins by Country:")
    print(shootout_wins)  

    # Query 3: Add match_key to all tables and display
    connection.execute("ALTER TABLE results_table ADD COLUMN match_key TEXT;")
    connection.execute("ALTER TABLE shootouts ADD COLUMN match_key TEXT;")
    connection.execute("ALTER TABLE goalscorers ADD COLUMN match_key TEXT;")
    connection.execute("UPDATE results_table SET match_key = date || home_team || away_team;")
    connection.execute("UPDATE shootouts SET match_key = date || home_team || away_team;")
    connection.execute("UPDATE goalscorers SET match_key = date || home_team || away_team;")

    # Sample Display of combined datasets to show results on output
    sample_combined_query = """
    SELECT 
        r.date AS match_date,
        r.home_team,
        r.away_team,
        r.tournament,
        r.home_score,
        r.away_score,
        s.winner AS shootout_winner,
        g.scorer AS goal_scorer,
        g.own_goal,
        g.penalty
    FROM 
        results_table r
    LEFT JOIN 
        shootouts s ON r.match_key = s.match_key
    LEFT JOIN 
        goalscorers g ON r.match_key = g.match_key
    LIMIT 10; 
    """
    #Limit was put on 10, due to too much data

    sample_combined_df = pd.read_sql_query(sample_combined_query, connection)

    # Display the sample rows
    print("Sample Combined Data with Match Keys:")
    print(sample_combined_df)

    

    # Query 4: Teams That Won a Penalty Shootout After a 1-1 Draw (with Dates)
    query_shootout_after_draw_with_dates = """
    SELECT 
        s.winner,
        r.date  -- Include the date of the match
    FROM 
        results_table r
    JOIN 
        shootouts s ON r.match_key = s.match_key
    WHERE 
        r.home_score = 1 AND r.away_score = 1
    ORDER BY 
        s.winner, r.date;
    """

   
    shootout_after_draw_with_dates = pd.read_sql_query(query_shootout_after_draw_with_dates, connection)

    print("Teams that Won a Penalty Shootout After a 1-1 Draw (with Dates):")
    print(shootout_after_draw_with_dates)  


   # Query 5: Get top scorers by tournament
    top_scorers_query = """
    WITH scorer_totals AS 
    (
    SELECT 
        r.tournament,
        g.scorer,
        COUNT(*) AS goals_scored
    FROM 
        goalscorers g
    JOIN 
        results_table r ON g.match_key = r.match_key
    GROUP BY 
        r.tournament, g.scorer
    ),
    top_scorers AS 
    (
    SELECT 
        st.tournament,
        st.scorer,
        st.goals_scored
    FROM 
        scorer_totals st
    WHERE st.goals_scored = (
        SELECT MAX(goals_scored)
        FROM scorer_totals st2
        WHERE st2.tournament = st.tournament)
    )
    SELECT 
        r.tournament,
        ts.scorer,
        ts.goals_scored
    FROM 
        results_table r
    LEFT JOIN 
        top_scorers ts ON r.tournament = ts.tournament
    GROUP BY 
        r.tournament, ts.scorer, ts.goals_scored
    ORDER BY 
        r.tournament;
    """

    top_scorers_df = pd.read_sql_query(top_scorers_query, connection)

    # Fetch the total goals scored in each tournament 
    total_goals_query = """
    SELECT 
        r.tournament,
        COUNT(*) AS total_goals
    FROM 
        goalscorers g
    JOIN 
        results_table r ON g.match_key = r.match_key
    GROUP BY 
        r.tournament;
    """

    total_goals_df = pd.read_sql_query(total_goals_query, connection)

    # Ensure all tournaments are included in the merge 
    merged_df = pd.merge(
        top_scorers_df, total_goals_df, on='tournament', how='outer'
    ).fillna({'scorer': 'No Scorer', 'goals_scored': 0, 'total_goals': 0})

    # Calculate percentages, also Avoids dividing by zero by replacing 0s with 1s
    merged_df['percentage_of_tournament_goals'] = (
        merged_df['goals_scored'] / merged_df['total_goals'].replace(0, 1) * 100
    )

    # Prints the final results
    print("Top Scorers with Percentage of Tournament Goals (including all tournaments):")
    print(merged_df)

