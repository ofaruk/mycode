SELECT
            b.Vehicle_ID,
            b.Option_Quantities,
            b.Options_Code,
            b.Option_Desc,
            b.Model_Text,
            b.Sales_Price,
            COALESCE(ZeroCost, b.Option_Quantities*Material_Cost, b.Option_Quantities*av.Average_Cost, Sales_Price*0.45) as Production_Cost,
            --below depends on business decision e.g. if no profit is expected from options costing zero; negative or positive, then profit is assumed 0
            CASE  
              WHEN ZeroCost = 0 THEN 0
              ELSE Sales_Price - COALESCE(ZeroCost, b.Option_Quantities*Material_Cost, b.Option_Quantities*av.Average_Cost, Sales_Price*0.45)
            END  as Profit
        FROM
        (
            SELECT
                Vehicle_ID,
                Option_Quantities,
                Options_Code,
                Option_Desc,
                Model_Text,
                CONCAT(SPLIT(Model_Text, ' ')[OFFSET(0)], "_", Options_Code) AS ModelOptionKey,
                Sales_Price,
                CASE
                    WHEN Sales_Price <= 0 THEN 0
                    ELSE null
                END AS ZeroCost
            FROM base_dataset.base_table
        ) b
        -- left join the average cost due to multiple occurance of material cost for modeloptionkey in options table
        LEFT JOIN 
        (
            SELECT Concat(Model, "_", Option_Code) as ModelOptionKey, AVG(Material_Cost) as Material_Cost FROM options_dataset.options_table Group by 1
        ) o on b.ModelOptionKey = o.ModelOptionKey
        LEFT JOIN
        (
            SELECT Option_Code, AVG(Material_Cost) as Average_Cost FROM options_dataset.options_table GROUP BY Option_Code
        ) av on b.Options_Code = av.Option_Code