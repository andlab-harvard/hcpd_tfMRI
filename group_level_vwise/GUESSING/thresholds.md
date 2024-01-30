# Z-value cutoff for FWE p < .05

Will be the same for all.

```
> qnorm(.025/(59412+31870))
[1] -5.008783
```



# Z-value for Cohen’s d > .2

Should be the same for all assuming N is correct. 

We set N here to be the Naive degrees of freedom. This is approximate, of course.

$$
\begin{align}
\begin{split}
N &= 1189 \\
d &= \frac{Z}{ \sqrt{N} }\\
d \times \sqrt{N} &= Z \\
Z &= .2 \times \sqrt{1189} = 6.896376
\end{split}
\end{align}
$$

