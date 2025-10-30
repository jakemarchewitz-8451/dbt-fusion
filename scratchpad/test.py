import dbt_fusion

# dbt_fusion.invoke(["init", "--project-name", "jake"])
val = dbt_fusion.invoke(["deps"])
print(f"RETVAL Python = {val}")